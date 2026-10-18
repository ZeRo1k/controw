use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Query},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use enigo::{
    Coordinate, Enigo, Settings as EnigoSettings,
    Keyboard, Mouse,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Terminal,
};
use qrcode::render::unicode;
use qrcode::QrCode;

#[cfg(windows)]
use vigem_client::{Client as VigemClient, TargetId, XButtons, XGamepad, Xbox360Wired};


type Tx = mpsc::UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<usize, (u64, Tx)>>>;
type PlayerMap = Arc<Mutex<HashMap<usize, PlayerController>>>;
type RumbleTx = Arc<Mutex<Option<Tx>>>;
type TuiEvents = Arc<Mutex<std::collections::VecDeque<String>>>;

const MAX_PLAYERS: usize = 4;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(8);

const METRICS_LOG_INTERVAL: Duration = Duration::from_secs(10);
const MAX_TEXT_MESSAGE: usize = 4096;
const MAX_BINARY_MESSAGE: usize = 64;

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    ButtonPress { button: String },
    ButtonRelease { button: String },
    JoystickMove { stick: String, x: f32, y: f32 },
    MouseMove { dx: f32, dy: f32 },
    KeyText { text: String },
    Ping,
}

#[derive(Serialize)]
struct ServerWelcome {
    r#type: String,
    player_id: usize,
}

#[derive(Serialize)]
struct ServerPong {
    r#type: String,
}

#[derive(Serialize)]
struct ServerRumble {
    r#type: String,
    large: u8,
    small: u8,
}

struct Metrics {
    connections: AtomicU64,
    disconnects: AtomicU64,
    packets_received: AtomicU64,
    packets_dropped: AtomicU64,
    invalid_packets: AtomicU64,
    watchdog_resets: AtomicU64,
    rumble_messages: AtomicU64,
    input_watchdog_resets: AtomicU64,
    stale_connections: AtomicU64,
}

impl Metrics {
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "connections": self.connections.load(Ordering::Relaxed),
            "disconnects": self.disconnects.load(Ordering::Relaxed),
            "packets_received": self.packets_received.load(Ordering::Relaxed),
            "packets_dropped": self.packets_dropped.load(Ordering::Relaxed),
            "invalid_packets": self.invalid_packets.load(Ordering::Relaxed),
            "watchdog_resets": self.watchdog_resets.load(Ordering::Relaxed),
            "rumble_messages": self.rumble_messages.load(Ordering::Relaxed),
            "input_watchdog_resets": self.input_watchdog_resets.load(Ordering::Relaxed),
            "stale_connections": self.stale_connections.load(Ordering::Relaxed),
        })
    }
}

#[derive(Clone)]
struct AppState {
    peers: PeerMap,
    players: PlayerMap,
    enigo: Arc<Mutex<Enigo>>,
    metrics: Arc<Metrics>,
    tui_events: TuiEvents,
}

#[derive(Default, Clone, Copy)]
struct GamepadButtons {
    dpad_up: bool,
    dpad_down: bool,
    dpad_left: bool,
    dpad_right: bool,
    start: bool,
    back: bool,
    lthumb: bool,
    rthumb: bool,
    lb: bool,
    rb: bool,
    guide: bool,
    a: bool,
    b: bool,
    x: bool,
    y: bool,
}

impl Drop for PlayerController {
    fn drop(&mut self) {
        #[cfg(windows)]
        if let Some(mut target) = self.xbox.take() {
            if let Err(e) = target.unplug() {
                eprintln!("[!] [P{}] Failed to unplug virtual Xbox controller: {:?}", self.player_id, e);
            } 
        }
    }
}

impl GamepadButtons {
    fn raw(&self) -> u16 {
        let mut v: u16 = 0;
        if self.dpad_up { v |= 0x0001; }
        if self.dpad_down { v |= 0x0002; }
        if self.dpad_left { v |= 0x0004; }
        if self.dpad_right { v |= 0x0008; }
        if self.start { v |= 0x0010; }
        if self.back { v |= 0x0020; }
        if self.lthumb { v |= 0x0040; }
        if self.rthumb { v |= 0x0080; }
        if self.lb { v |= 0x0100; }
        if self.rb { v |= 0x0200; }
        if self.guide { v |= 0x0400; }
        if self.a { v |= 0x1000; }
        if self.b { v |= 0x2000; }
        if self.x { v |= 0x4000; }
        if self.y { v |= 0x8000; }
        v
    }
}

#[derive(Default, Clone, Copy)]
struct GamepadState {
    buttons: GamepadButtons,
    left_trigger: u8,
    right_trigger: u8,
    thumb_lx: i16,
    thumb_ly: i16,
    thumb_rx: i16,
    thumb_ry: i16,
}

struct PlayerController {
    player_id: usize,
    connected: bool,
    connection_generation: u64,
    last_sequence: u16,
    has_received_packet: bool,
    client_id: Option<String>,
    gamepad: GamepadState,
    last_packet_at: Instant,
    connected_at: Instant,
    packet_count: u64,
    dropped_packets: u64,
    invalid_packets: u64,
    #[cfg(windows)]
    xbox: Option<Xbox360Wired<VigemClient>>,
    rumble_tx: RumbleTx,
}

impl PlayerController {
    fn new(player_id: usize) -> Self {
        Self {
            player_id,
            connected: false,
            connection_generation: 0,
            last_sequence: 0,
            has_received_packet: false,
            client_id: None,
            gamepad: GamepadState::default(),
            last_packet_at: Instant::now(),
            connected_at: Instant::now(),
            packet_count: 0,
            dropped_packets: 0,
            invalid_packets: 0,
            #[cfg(windows)]
            xbox: None,
            rumble_tx: Arc::new(Mutex::new(None)),
        }
    }

    async fn attach(&mut self, tx: Tx) -> Result<u64, String> {
        #[cfg(windows)]
        if self.xbox.is_none() {
            self.xbox = create_xbox_controller(self.player_id, self.rumble_tx.clone());
            if self.xbox.is_none() {
                return Err("failed to create virtual controller".into());
            }
        }

        self.connection_generation = self.connection_generation.wrapping_add(1).max(1);
        self.connected = true;
        self.last_sequence = 0;
        self.has_received_packet = false;
        self.last_packet_at = Instant::now();
        self.connected_at = Instant::now();
        self.packet_count = 0;
        self.dropped_packets = 0;
        self.invalid_packets = 0;
        *self.rumble_tx.lock().await = Some(tx);
        self.gamepad = GamepadState::default();
        push_gamepad_update(self);
        Ok(self.connection_generation)
    }

    async fn detach(&mut self) {
        self.connection_generation = self.connection_generation.wrapping_add(1).max(1);
        self.connected = false;
        *self.rumble_tx.lock().await = None;
        self.last_sequence = 0;
        self.has_received_packet = false;
        self.gamepad = GamepadState::default();
        push_gamepad_update(self);

        #[cfg(windows)]
        if let Some(mut target) = self.xbox.take() {
            let _ = target.unplug();
        }
    }

    
}


#[cfg(windows)]
fn create_xbox_controller(
    player_id: usize,
    rumble_tx: RumbleTx,
) -> Option<Xbox360Wired<VigemClient>> {
    let client = match VigemClient::connect() {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[!] [P{}] Could not connect to ViGEmBus driver: {:?}",
                player_id, e
            );
            return None;
        }
    };

    let mut target =
        Xbox360Wired::new(client, TargetId::XBOX360_WIRED);

    if let Err(e) = target.plugin() {
        eprintln!(
            "[!] [P{}] Failed to plug in virtual Xbox controller: {:?}",
            player_id, e
        );
        return None;
    }

    if let Err(e) = target.wait_ready() {
        eprintln!(
            "[!] [P{}] Virtual Xbox controller never became ready: {:?}",
            player_id, e
        );
        return None;
    }

    match target.request_notification() {
        Ok(notification) => {
            let p_id = player_id;
            let rumble_tx = rumble_tx.clone();

            notification.spawn_thread(
                move |_request, rumble| {
                    let rumble_msg = ServerRumble {
                        r#type: "rumble".to_string(),
                        large: rumble.large_motor,
                        small: rumble.small_motor,
                    };

                    let json = match serde_json::to_string(&rumble_msg) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[!] [P{}] Failed to serialize rumble: {:?}", p_id, e);
                            return;
                        }
                    };

                    // The virtual controller stays plugged into its permanent slot.
                    // Only the currently connected phone receives rumble.
                    if let Ok(guard) = rumble_tx.try_lock() {
                        if let Some(tx) = guard.as_ref() {
                            let _ = tx.send(Message::Text(json.into()));
                        }
                    }
                },
            );

            
        }
        Err(e) => {
            eprintln!("[!] [P{}] Failed to request rumble notifications: {:?}", player_id, e);
        }
    }


    Some(target)
}

fn generate_qr_code(url: &str) -> String {
    match QrCode::new(url.as_bytes()) {
        Ok(code) => code
            .render::<unicode::Dense1x2>()
            .dark_color(unicode::Dense1x2::Light)
            .light_color(unicode::Dense1x2::Dark)
            .build(),
        Err(_) => "Failed to render QR Code".to_string(),
    }
}

#[cfg(windows)]
fn push_gamepad_update(controller: &mut PlayerController) {
    if let Some(target) = controller.xbox.as_mut() {
        let g = &controller.gamepad;
        let report = XGamepad {
            buttons: XButtons { raw: g.buttons.raw() },
            left_trigger: g.left_trigger,
            right_trigger: g.right_trigger,
            thumb_lx: g.thumb_lx,
            thumb_ly: g.thumb_ly,
            thumb_rx: g.thumb_rx,
            thumb_ry: g.thumb_ry,
        };
        if let Err(e) = target.update(&report) {
            eprintln!("[!] [P{}] Failed to update virtual controller: {:?}", controller.player_id, e);
        }
    }
}

#[cfg(not(windows))]
fn push_gamepad_update(_controller: &mut PlayerController) {}

fn apply_heuristic_stick(raw_x: f32, raw_y: f32) -> (i16, i16) {
    // Controller-grade radial deadzone + response curve.
    // Keeps diagonals circular, removes tiny touch jitter, and preserves
    // full travel at the edge instead of crushing the stick range.
    let x = raw_x.clamp(-1.0, 1.0);
    let y = raw_y.clamp(-1.0, 1.0);
    let mag = (x * x + y * y).sqrt();
    const DEADZONE: f32 = 0.055;

    if mag <= DEADZONE {
        return (0, 0);
    }

    let normalized = ((mag - DEADZONE) / (1.0 - DEADZONE)).clamp(0.0, 1.0);
    // Slightly softer center for precise aiming, while keeping max output.
    let curved = normalized * normalized * (3.0 - 2.0 * normalized);
    let scale = curved / mag;

    let out_x = x * scale;
    let out_y = -(y * scale);

    (
        (out_x * 32767.0).round().clamp(-32767.0, 32767.0) as i16,
        (out_y * 32767.0).round().clamp(-32767.0, 32767.0) as i16,
    )
}

fn apply_button(controller: &mut PlayerController, button: &str, pressed: bool) {
    match button {
        "DPAD_UP" => controller.gamepad.buttons.dpad_up = pressed,
        "DPAD_DOWN" => controller.gamepad.buttons.dpad_down = pressed,
        "DPAD_LEFT" => controller.gamepad.buttons.dpad_left = pressed,
        "DPAD_RIGHT" => controller.gamepad.buttons.dpad_right = pressed,

        "A" | "JUMP" => controller.gamepad.buttons.a = pressed,
        "B" => controller.gamepad.buttons.b = pressed,
        "X" | "COMBAT_X" | "RACING_X" => controller.gamepad.buttons.x = pressed,
        "Y" | "H" => controller.gamepad.buttons.y = pressed,

        "LB" => controller.gamepad.buttons.lb = pressed,
        "RB" | "R" => controller.gamepad.buttons.rb = pressed,
        "LS" => controller.gamepad.buttons.lthumb = pressed,
        "RS" => controller.gamepad.buttons.rthumb = pressed,
        "BACK" => controller.gamepad.buttons.back = pressed,
        "START" => controller.gamepad.buttons.start = pressed,
        "GUIDE" => controller.gamepad.buttons.guide = pressed,

        "LT" | "COMBAT_TARGET" | "BRAKE" => {
            controller.gamepad.left_trigger = if pressed { 255 } else { 0 };
        }
        "RT" | "FIRE" | "ACCELERATE" => {
            controller.gamepad.right_trigger = if pressed { 255 } else { 0 };
        }
        _ => {}
    }
    push_gamepad_update(controller);
}

async fn tui_event(events: &TuiEvents, message: impl Into<String>) {
    let mut guard = events.lock().await;
    guard.push_back(message.into());
    while guard.len() > 32 {
        guard.pop_front();
    }
}

async fn run_tui(state: AppState, url: String) -> std::io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let qr_code_str = generate_qr_code(&url);

    let result = async {
        loop {
            if event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    if matches!(key.code, KeyCode::Char('q') | KeyCode::Char('Q')) {
                        break Ok(());
                    }
                }
            }

            let players = state.players.lock().await;
            let metrics = state.metrics.snapshot();
            let active = players.values().filter(|p| p.connected).count();
            let rows: Vec<Row> = (1..=MAX_PLAYERS)
                .map(|id| {
                    if let Some(p) = players.get(&id) {
                        let status = if p.connected { "CONNECTED" } else { "AVAILABLE" };
                        let uptime = if p.connected {
                            format!("{}s", p.connected_at.elapsed().as_secs())
                        } else {
                            "-".to_string()
                        };
                        Row::new(vec![
                            Cell::from(format!("P{}", p.player_id)),
                            Cell::from(status),
                            Cell::from(uptime),
                            Cell::from(p.packet_count.to_string()),
                            Cell::from(p.dropped_packets.to_string()),
                            Cell::from(p.invalid_packets.to_string()),
                        ])
                    } else {
                        Row::new(vec![
                            Cell::from(format!("P{}", id)),
                            Cell::from("MISSING"),
                            Cell::from("-"),
                            Cell::from("-"),
                            Cell::from("-"),
                            Cell::from("-"),
                        ])
                    }
                })
                .collect();
            drop(players);

            let events_text = {
                let events = state.tui_events.lock().await;
                if events.is_empty() {
                    "No events yet".to_string()
                } else {
                    events.iter().cloned().collect::<Vec<_>>().join("\n")
                }
            };

            let connections = metrics["connections"].as_u64().unwrap_or(0);
            let disconnects = metrics["disconnects"].as_u64().unwrap_or(0);
            let dropped = metrics["packets_dropped"].as_u64().unwrap_or(0);
            let invalid = metrics["invalid_packets"].as_u64().unwrap_or(0);

            terminal.draw(|frame| {
                let root = frame.area();
                
                // 1. Split the entire screen into Left (Data) and Right (QR Code)
                let main_split = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Min(65),    // Left column takes up remaining space
                        Constraint::Length(34), // Right column is fixed width for the QR code
                    ])
                    .split(root);

                // 2. Split the Left column vertically into Header, Table, and Log
                let left_column = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(13), // Server Info block
                        Constraint::Length(8),  // Connected players block
                        Constraint::Min(5),     // Events log gets the rest of the height
                    ])
                    .split(main_split[0]);

                let header = Paragraph::new(format!(
                    "CONTROLLER SERVER\n\nURL:\n{}\n\nPlayers: {}/{}\nConnections: {}\nDisconnects: {}\nDropped: {}\nInvalid: {}\n\nPress Q to quit",
                    url, active, MAX_PLAYERS, connections, disconnects, dropped, invalid
                ))
                .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
                .block(Block::default().borders(Borders::ALL).title("Server Info"));
                
                frame.render_widget(header, left_column[0]);

                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(8),
                        Constraint::Length(14),
                        Constraint::Length(12),
                        Constraint::Length(14),
                        Constraint::Length(14),
                        Constraint::Length(14),
                    ],
                )
                .header(
                    Row::new(vec!["PLAYER", "STATUS", "UPTIME", "PACKETS", "DROPPED", "INVALID"])
                        .style(Style::default().add_modifier(Modifier::BOLD)),
                )
                .block(Block::default().borders(Borders::ALL).title("Connected Players"));
                
                frame.render_widget(table, left_column[1]);

                let log = Paragraph::new(events_text)
                    .style(Style::default().fg(Color::White))
                    .block(Block::default().borders(Borders::ALL).title("Events"));
                    
                frame.render_widget(log, left_column[2]);

                // 3. Render the QR code in the Right column (spanning top to bottom)
                let qr_widget = Paragraph::new(qr_code_str.as_str())
                    .style(Style::default().fg(Color::White))
                    .block(Block::default().borders(Borders::ALL).title("Scan to Connect"));
                    
                frame.render_widget(qr_widget, main_split[1]);
            })?;

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }.await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

#[tokio::main]
async fn main() {
    let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));
    let metrics = Arc::new(Metrics {
        connections: AtomicU64::new(0),
        disconnects: AtomicU64::new(0),
        packets_received: AtomicU64::new(0),
        packets_dropped: AtomicU64::new(0),
        invalid_packets: AtomicU64::new(0),
        watchdog_resets: AtomicU64::new(0),
        rumble_messages: AtomicU64::new(0),
        input_watchdog_resets: AtomicU64::new(0),
        stale_connections: AtomicU64::new(0),
    });
    let mut initial_players = HashMap::new();
    for id in 1..=MAX_PLAYERS {
        initial_players.insert(id, PlayerController::new(id));
    }
    let players: PlayerMap = Arc::new(Mutex::new(initial_players));

    let enigo = Enigo::new(&EnigoSettings::default())
        .expect("Failed to initialize mouse/keyboard control (enigo)");

    #[cfg(not(windows))]
    eprintln!("[!] Virtual Xbox controller emulation (ViGEmBus) is Windows-only; skipping on this OS.");

    let tui_events: TuiEvents = Arc::new(Mutex::new(std::collections::VecDeque::new()));
    let state = AppState {
        peers,
        players: players.clone(),
        enigo: Arc::new(Mutex::new(enigo)),
        metrics: metrics.clone(),
        tui_events: tui_events.clone(),
    };

    {
        let watchdog_players = players.clone();
        let metrics_for_watchdog = metrics.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                ticker.tick().await;
                let mut guard = watchdog_players.lock().await;
                for controller in guard.values_mut() {
                    if controller.connected && controller.last_packet_at.elapsed() > CONNECTION_TIMEOUT {
                        eprintln!("[!] [P{}] Input watchdog timeout; neutralizing controller", controller.player_id);
                        metrics_for_watchdog.watchdog_resets.fetch_add(1, Ordering::Relaxed);
                        metrics_for_watchdog.input_watchdog_resets.fetch_add(1, Ordering::Relaxed);
                        controller.detach().await;
                    }
                }
            }
        });
    }


    {
        
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(METRICS_LOG_INTERVAL);
            loop {
                ticker.tick().await;
                
            }
        });
    }

    let my_local_ip = StdUdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| {
            socket.connect("8.8.8.8:80")?;
            socket.local_addr().map(|addr| addr.ip())
        })
        .unwrap_or_else(|_| IpAddr::V4(Ipv4Addr::LOCALHOST));

    let port = 8080;
    let url = format!("http://{}:{}/", my_local_ip, port);

    tui_event(&tui_events, format!("Server listening at {}", url)).await;
    tui_event(&tui_events, "Ready for controller connections").await;

    let state_health = state.clone();
    let state_stats = state.clone();
    let tui_state = state.clone();
    let app = Router::new()
        .route("/", get(serve_html))
        .route("/health", get(move || health_handler(state_health.clone())))
        .route("/stats", get(move || stats_handler(state_stats.clone())))
        .route(
            "/ws",
            get(move |ws: WebSocketUpgrade, Query(query): Query<HashMap<String, String>>| {
                let client_id = query.get("cid").filter(|c| !c.is_empty() && c.len() <= 64).cloned();
                ws_handler(ws, client_id, state)
            }),
        );
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap_or_else(|e| {
        eprintln!("Failed to bind {}: {}", addr, e);
        std::process::exit(1);
    });

    
    let tui_url = url.clone();
    let tui_task = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Handle::current();
        runtime.block_on(run_tui(tui_state, tui_url))
    });

    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                eprintln!("HTTP server stopped: {}", e);
            }
        }
        result = tui_task => {
            if let Err(e) = result {
                eprintln!("TUI task stopped: {}", e);
            }
        }
    }
}

async fn serve_html() -> impl IntoResponse {
    Html(CONTROLLER_HTML)
}

async fn health_handler(state: AppState) -> impl IntoResponse {
    let players = state.players.lock().await;
    let active = players.values().filter(|p| p.connected).count();
    axum::Json(serde_json::json!({
        "status": "ok",
        "active_players": active,
        "capacity": MAX_PLAYERS
    }))
}

async fn stats_handler(state: AppState) -> impl IntoResponse {
    let players = state.players.lock().await;
    let slots: Vec<_> = players.values().map(|p| serde_json::json!({
        "player": p.player_id,
        "connected": p.connected,
        "generation": p.connection_generation,
        "packets": p.packet_count,
        "dropped_packets": p.dropped_packets,
        "invalid_packets": p.invalid_packets,
        "uptime_ms": if p.connected { p.connected_at.elapsed().as_millis() } else { 0 }
    })).collect();
    axum::Json(serde_json::json!({
        "status": state.metrics.snapshot(),
        "slots": slots
    }))
}

async fn ws_handler(ws: WebSocketUpgrade, client_id: Option<String>, state: AppState) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, client_id, state))
}

async fn handle_socket(stream: WebSocket, client_id: Option<String>, state: AppState) {
    let (mut sender, mut receiver) = stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel();

    let (player_id, connection_generation) = {
        let mut players = state.players.lock().await;
        // 1) A phone that reconnects (same client id) gets its own slot back,
        //    even if the server has not noticed the old socket died yet; the
        //    stale socket is superseded by the new connection generation.
        // 2) Otherwise take the lowest free slot.
        let id = client_id
            .as_ref()
            .and_then(|cid| {
                players
                    .iter()
                    .filter(|(_, c)| c.client_id.as_deref() == Some(cid.as_str()))
                    .map(|(id, _)| *id)
                    .min()
            })
            .or_else(|| {
                players
                    .iter()
                    .filter(|(_, controller)| !controller.connected)
                    .map(|(id, _)| *id)
                    .min()
            });

        let Some(id) = id else {
            let _ = sender.send(Message::Text(serde_json::json!({
                "type": "server_full",
                "max_players": MAX_PLAYERS
            }).to_string().into())).await;
            return;
        };

        let generation = if let Some(controller) = players.get_mut(&id) {
            match controller.attach(tx.clone()).await {
                Ok(generation) => {
                    controller.client_id = client_id.clone();
                    generation
                }
                Err(error) => {
                    eprintln!("player {}: {}", id, error);
                    let _ = sender.send(Message::Text(serde_json::json!({
                        "type": "controller_unavailable"
                    }).to_string().into())).await;
                    return;
                }
            }
        } else {
            return;
        };

        (id, generation)
    };

    state.metrics.connections.fetch_add(1, Ordering::Relaxed);
    tui_event(&state.tui_events, format!("P{} connected (generation {})", player_id, connection_generation)).await;

    let welcome = serde_json::to_string(&ServerWelcome {
        r#type: "welcome".into(),
        player_id,
    })
    .unwrap();

    let _ = tx.send(Message::Text(welcome.into()));
    state.peers.lock().await.insert(player_id, (connection_generation, tx.clone()));

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Binary(bin) => {
                state.metrics.packets_received.fetch_add(1, Ordering::Relaxed);
                if bin.len() >= 12 && bin.len() <= MAX_BINARY_MESSAGE {
                    // The browser must use the server-assigned slot. Never trust a
                    // client-supplied player number to address another controller.
                    if bin[0] as usize != player_id {
                        state.metrics.invalid_packets.fetch_add(1, Ordering::Relaxed);
                        if let Some(ctrl) = state.players.lock().await.get_mut(&player_id) { ctrl.invalid_packets += 1; }
                        continue;
                    }
                    let seq = u16::from_be_bytes([bin[1], bin[2]]);
                    let msg_type = bin[3];
                    let mut players_guard = state.players.lock().await;
                    let Some(ctrl) = players_guard.get_mut(&player_id) else { break; };
                    if !ctrl.connected || ctrl.connection_generation != connection_generation {
                        state.metrics.packets_dropped.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    if ctrl.has_received_packet {
                        let delta = seq.wrapping_sub(ctrl.last_sequence);
                        if delta == 0 || delta > 0x8000 {
                            ctrl.dropped_packets = ctrl.dropped_packets.saturating_add(1);
                            state.metrics.packets_dropped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                    ctrl.last_sequence = seq;
                    ctrl.has_received_packet = true;
                    ctrl.packet_count = ctrl.packet_count.saturating_add(1);
                    ctrl.last_packet_at = Instant::now();

                    if msg_type == 1 {
                        let raw_btn = u16::from_be_bytes([bin[4], bin[5]]);
                        let lt = bin[6];
                        let rt = bin[7];
                        let lx_i8 = bin[8] as i8;
                        let ly_i8 = bin[9] as i8;
                        let rx_i8 = bin[10] as i8;
                        let ry_i8 = bin[11] as i8;

                        let lx_norm = (lx_i8 as f32) / 127.0;
                        let ly_norm = (ly_i8 as f32) / 127.0;
                        let rx_norm = (rx_i8 as f32) / 127.0;
                        let ry_norm = (ry_i8 as f32) / 127.0;

                        let (lx, ly) = apply_heuristic_stick(lx_norm, ly_norm);
                        let (rx, ry) = apply_heuristic_stick(rx_norm, ry_norm);

                        {
                            ctrl.gamepad.buttons.dpad_up = (raw_btn & 0x0001) != 0;
                            ctrl.gamepad.buttons.dpad_down = (raw_btn & 0x0002) != 0;
                            ctrl.gamepad.buttons.dpad_left = (raw_btn & 0x0004) != 0;
                            ctrl.gamepad.buttons.dpad_right = (raw_btn & 0x0008) != 0;
                            ctrl.gamepad.buttons.start = (raw_btn & 0x0010) != 0;
                            ctrl.gamepad.buttons.back = (raw_btn & 0x0020) != 0;
                            ctrl.gamepad.buttons.lthumb = (raw_btn & 0x0040) != 0;
                            ctrl.gamepad.buttons.rthumb = (raw_btn & 0x0080) != 0;
                            ctrl.gamepad.buttons.lb = (raw_btn & 0x0100) != 0;
                            ctrl.gamepad.buttons.rb = (raw_btn & 0x0200) != 0;
                            ctrl.gamepad.buttons.guide = (raw_btn & 0x0400) != 0;
                            ctrl.gamepad.buttons.a = (raw_btn & 0x1000) != 0;
                            ctrl.gamepad.buttons.b = (raw_btn & 0x2000) != 0;
                            ctrl.gamepad.buttons.x = (raw_btn & 0x4000) != 0;
                            ctrl.gamepad.buttons.y = (raw_btn & 0x8000) != 0;

                            ctrl.gamepad.left_trigger = lt;
                            ctrl.gamepad.right_trigger = rt;
                            ctrl.gamepad.thumb_lx = lx;
                            ctrl.gamepad.thumb_ly = ly;
                            ctrl.gamepad.thumb_rx = rx;
                            ctrl.gamepad.thumb_ry = ry;

                            push_gamepad_update(ctrl);
                        }
                    }
                } else {
                    state.metrics.invalid_packets.fetch_add(1, Ordering::Relaxed);
                    let mut players = state.players.lock().await;
                    if let Some(ctrl) = players.get_mut(&player_id) { ctrl.invalid_packets += 1; }
                }
            }
            Message::Text(text) => {
                if text.len() > MAX_TEXT_MESSAGE {
                    state.metrics.invalid_packets.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if let Ok(parsed) = serde_json::from_str::<ClientMessage>(&text) {
                    match parsed {
                        ClientMessage::ButtonPress { button } => {
                            let mut players = state.players.lock().await;
                            if let Some(ctrl) = players.get_mut(&player_id) {
                                if ctrl.connected && ctrl.connection_generation == connection_generation {
                                    apply_button(ctrl, &button, true);
                                    ctrl.last_packet_at = Instant::now();
                                }
                            }
                        }
                        ClientMessage::ButtonRelease { button } => {
                            let mut players = state.players.lock().await;
                            if let Some(ctrl) = players.get_mut(&player_id) {
                                if ctrl.connected && ctrl.connection_generation == connection_generation {
                                    apply_button(ctrl, &button, false);
                                    ctrl.last_packet_at = Instant::now();
                                }
                            }
                        }
                        ClientMessage::JoystickMove { stick, x, y } => {
                            let mut players = state.players.lock().await;
                            if let Some(ctrl) = players.get_mut(&player_id) {
                                if !ctrl.connected || ctrl.connection_generation != connection_generation {
                                    continue;
                                }
                                ctrl.last_packet_at = Instant::now();
                                if stick == "right" {
                                    let (rx, ry) = apply_heuristic_stick(x, y);
                                    ctrl.gamepad.thumb_rx = rx;
                                    ctrl.gamepad.thumb_ry = ry;
                                } else {
                                    let (lx, ly) = apply_heuristic_stick(x, y);
                                    ctrl.gamepad.thumb_lx = lx;
                                    ctrl.gamepad.thumb_ly = ly;
                                }
                                push_gamepad_update(ctrl);
                            }
                        }
                        ClientMessage::MouseMove { dx, dy } => {
                            let mut enigo = state.enigo.lock().await;
                            let _ = enigo.move_mouse(dx.round() as i32, dy.round() as i32, Coordinate::Rel);
                        }
                        ClientMessage::KeyText { text } => {
                            let mut enigo = state.enigo.lock().await;
                            let _ = enigo.text(&text);
                        }
                        ClientMessage::Ping => {
                            let mut players = state.players.lock().await;
                            if let Some(ctrl) = players.get_mut(&player_id) {
                                if ctrl.connected && ctrl.connection_generation == connection_generation {
                                    ctrl.last_packet_at = Instant::now();
                                }
                            }
                            drop(players);
                            let pong = serde_json::to_string(&ServerPong {
                                r#type: "pong".into(),
                            }).unwrap();
                            let _ = tx.send(Message::Text(pong.into()));
                        }
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    {
        let mut players = state.players.lock().await;
        if let Some(ctrl) = players.get_mut(&player_id) {
            if ctrl.connection_generation == connection_generation {
                ctrl.detach().await;
            }
        }
    }
    {
        let mut peers = state.peers.lock().await;
        // A slot can be reused before an old websocket handler finishes.
        // Only remove the peer belonging to this connection generation.
        if peers
            .get(&player_id)
            .map(|(generation, _)| *generation == connection_generation)
            .unwrap_or(false)
        {
            peers.remove(&player_id);
        }
    }
    state.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
    tui_event(&state.tui_events, format!("P{} disconnected", player_id)).await;
}

const CONTROLLER_HTML: &str = include_str!("controller.html");
