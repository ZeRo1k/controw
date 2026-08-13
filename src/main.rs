use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
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
            } else {
                println!("[-] [P{}] Virtual Xbox controller cleanly unplugged from ViGEmBus", self.player_id);
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

            println!("[+] [P{}] Rumble notifications enabled", player_id);
        }
        Err(e) => {
            eprintln!("[!] [P{}] Failed to request rumble notifications: {:?}", player_id, e);
        }
    }

    println!(
        "[+] [P{}] Virtual Xbox 360 controller connected via ViGEmBus (Rumble Active)",
        player_id
    );

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
        .route("/ws", get(move |ws| ws_handler(ws, state)));
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

async fn ws_handler(ws: WebSocketUpgrade, state: AppState) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(stream: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel();

    let (player_id, connection_generation) = {
        let mut players = state.players.lock().await;
        let id = players
            .iter()
            .filter(|(_, controller)| !controller.connected)
            .map(|(id, _)| *id)
            .min();

        let Some(id) = id else {
            let _ = tx.send(Message::Text(serde_json::json!({
                "type": "server_full",
                "max_players": MAX_PLAYERS
            }).to_string().into()));
            return;
        };

        let generation = if let Some(controller) = players.get_mut(&id) {
            match controller.attach(tx.clone()).await {
                Ok(generation) => generation,
                Err(error) => {
                    eprintln!("player {}: {}", id, error);
                    let _ = tx.send(Message::Text(serde_json::json!({
                        "type": "controller_unavailable"
                    }).to_string().into()));
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

const CONTROLLER_HTML: &str = r##"
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">
<meta name="theme-color" content="#1b1b1b">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>Xbox Controller V2</title>
<style>
:root {
    --bg-color: #1b1b1b;
    --btn-fill: #6e6e6e;
    --btn-border: #363636;
    --btn-text: #1a1a1a;
    --active-glow: #4cd964;
    --unlinked-glow: #888888;
    --knob-bg: #6e6e6e;
    --stick-outer: #2c2c2c;
}

* {
    box-sizing: border-box;
    -webkit-tap-highlight-color: transparent;
    user-select: none;
    -webkit-user-select: none;
    touch-action: none;
}

html, body {
    margin: 0;
    padding: 0;
    width: 100vw;
    height: 100dvh;
    overflow: hidden;
    background-color: var(--bg-color);
    color: var(--btn-text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
}

.controller-stage {
    position: relative;
    width: 100vw;
    height: 100dvh;
    overflow: hidden;
    background: var(--bg-color);
}

/* Button General */
.btn {
    position: absolute;
    border: clamp(2px, 0.4vw, 5px) solid var(--btn-border);
    background-color: var(--btn-fill);
    color: var(--btn-text);
    font-weight: 900;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    box-shadow: inset 0 2px 3px rgba(255,255,255,0.18), 0 4px 8px rgba(0,0,0,0.5);
    transition: transform 0.05s ease, filter 0.05s ease, border-color 0.1s ease;
    z-index: 5;
}

.btn:active, .btn.pressed {
    transform: scale(0.90) !important;
    filter: brightness(0.75);
    background-color: #555555;
}

.btn.turbo-active {
    border-color: #ff9500 !important;
    box-shadow: 0 0 16px #ff9500, inset 0 0 8px #ff9500 !important;
    animation: turboPulse 0.1s infinite alternate;
}
@keyframes turboPulse {
    0% { filter: brightness(1.3); }
    100% { filter: brightness(0.85); }
}

.btn-round { border-radius: 50%; }

/* --- LEFT SIDE --- */

.btn-ls {
    left: clamp(10px, 1.8vw, 24px);
    top: 50%;
    transform: translateY(-50%);
    width: clamp(34px, 5.2vw, 52px);
    height: clamp(34px, 5.2vw, 52px);
    font-size: clamp(11px, 1.5vw, 16px);
}

.dpad {
    position: absolute;
    left: clamp(50px, 11vw, 160px);
    top: clamp(8px, 4%, 30px);
    width: clamp(100px, 15.5vw, 160px);
    height: clamp(100px, 15.5vw, 160px);
    z-index: 5;
}

.dpad-btn {
    position: absolute;
    background-color: var(--btn-fill);
    border: clamp(2px, 0.4vw, 5px) solid var(--btn-border);
    box-shadow: inset 0 2px 3px rgba(255,255,255,0.18), 0 4px 8px rgba(0,0,0,0.4);
    cursor: pointer;
}

.dpad-btn:active, .dpad-btn.pressed {
    filter: brightness(0.75);
    transform: scale(0.92);
}

.dpad-btn.turbo-active {
    border-color: #ff9500 !important;
    box-shadow: 0 0 16px #ff9500, inset 0 0 8px #ff9500 !important;
}

.dpad-up {
    top: 0; left: 32%; width: 36%; height: 36%;
    border-radius: 42% 42% 14% 14%;
}

.dpad-down {
    bottom: 0; left: 32%; width: 36%; height: 36%;
    border-radius: 14% 14% 42% 42%;
}

.dpad-left {
    top: 32%; left: 0; width: 36%; height: 36%;
    border-radius: 42% 14% 14% 42%;
}

.dpad-right {
    top: 32%; right: 0; width: 36%; height: 36%;
    border-radius: 14% 42% 42% 14%;
}

.dpad-center {
    position: absolute;
    top: 32%; left: 32%; width: 36%; height: 36%;
    background-color: var(--btn-fill);
    border: clamp(2px, 0.4vw, 5px) solid var(--btn-border);
    border-radius: 14%;
    pointer-events: none;
}

.left-stick {
    left: clamp(18px, 5.5vw, 85px);
    bottom: clamp(8px, 4%, 30px);
}

.btn-back {
    left: clamp(170px, 29%, 400px);
    top: clamp(20px, 18%, 110px);
    width: clamp(34px, 5vw, 50px);
    height: clamp(34px, 5vw, 50px);
}

.btn-lb {
    left: clamp(165px, 27.5%, 380px);
    top: clamp(130px, 46%, 250px);
    width: clamp(40px, 6vw, 60px);
    height: clamp(40px, 6vw, 60px);
    font-size: clamp(12px, 1.6vw, 17px);
}

.btn-lt {
    left: clamp(165px, 27.5%, 380px);
    top: clamp(190px, 70%, 370px);
    width: clamp(40px, 6vw, 60px);
    height: clamp(40px, 6vw, 60px);
    font-size: clamp(12px, 1.6vw, 17px);
}

/* --- CENTER --- */

.btn-guide {
    left: 50%;
    top: clamp(15px, 22%, 120px);
    transform: translate(-50%, -50%);
    width: clamp(46px, 7vw, 72px);
    height: clamp(46px, 7vw, 72px);
    border-radius: 50%;
    background-color: var(--btn-fill);
    border: clamp(3px, 0.5vw, 5px) solid var(--btn-border);
}

.guide-inner {
    width: 70%; height: 70%;
    border-radius: 50%;
    background-color: #2b2b2b;
    color: var(--btn-fill);
    display: flex; align-items: center; justify-content: center;
}

.player-pill {
    /* Small circular player slot. Placement is collision-aware in JS. */
    position: absolute;
    left: auto;
    right: max(8px, env(safe-area-inset-right));
    top: max(8px, env(safe-area-inset-top));
    transform: none;
    width: clamp(36px, 5vw, 48px);
    height: clamp(36px, 5vw, 48px);
    padding: 0;
    border-radius: 50%;
    background-color: #2b2b2b;
    border: 2px solid #444;
    color: #dedede;
    font-weight: 900;
    font-size: clamp(13px, 1.7vw, 18px);
    letter-spacing: 0;
    box-shadow: 0 4px 10px rgba(0,0,0,0.3);
    display: flex;
    align-items: center;
    justify-content: center;
    white-space: nowrap;
    z-index: 30;
    cursor: pointer;
    transition: left 0.15s ease, right 0.15s ease, top 0.15s ease,
                bottom 0.15s ease, background-color 0.15s ease,
                border-color 0.15s ease, transform 0.1s ease;
    width: 44px;
    height: 44px;
    padding: 0;
    border-radius: 50%;
    box-sizing: border-box;
    gap: 0;
}

.player-pill:active {
    transform: scale(0.93);
}

.status-indicator {
    display: none;
}

.player-pill.connected .status-indicator {
    display: none;
}

.player-pill.player-collision-safe {
    box-shadow: 0 0 0 2px rgba(255,255,255,0.04), 0 4px 10px rgba(0,0,0,0.3);
}

.btn-start {
    left: clamp(210px, 60%, 750px);
    top: clamp(20px, 18%, 110px);
    width: clamp(34px, 5vw, 50px);
    height: clamp(34px, 5vw, 50px);
}

/* --- RIGHT WING --- */

.btn-rb {
    right: clamp(165px, 27.5%, 380px);
    top: clamp(130px, 46%, 250px);
    width: clamp(40px, 6vw, 60px);
    height: clamp(40px, 6vw, 60px);
    font-size: clamp(12px, 1.6vw, 17px);
}

.btn-rt {
    right: clamp(165px, 27.5%, 380px);
    top: clamp(190px, 70%, 370px);
    width: clamp(40px, 6vw, 60px);
    height: clamp(40px, 6vw, 60px);
    font-size: clamp(12px, 1.6vw, 17px);
}

.right-stick {
    right: clamp(45px, 10vw, 150px);
    top: clamp(8px, 4%, 30px);
}

.btn-rs {
    right: clamp(10px, 1.8vw, 24px);
    top: 50%;
    transform: translateY(-50%);
    width: clamp(34px, 5.2vw, 52px);
    height: clamp(34px, 5.2vw, 52px);
    font-size: clamp(11px, 1.5vw, 16px);
}

.abxy-cluster {
    position: absolute;
    right: clamp(18px, 4.5vw, 75px);
    bottom: clamp(8px, 4%, 30px);
    width: clamp(110px, 17vw, 180px);
    height: clamp(110px, 17vw, 180px);
    z-index: 5;
}

.btn-abxy {
    position: absolute;
    width: 36%; height: 36%;
    font-size: clamp(15px, 2.3vw, 24px);
}

.btn-y { top: 0; left: 32%; }
.btn-x { top: 32%; left: 0; }
.btn-b { top: 32%; right: 0; }
.btn-a { bottom: 0; left: 32%; }

/* Joysticks Common */
.stick-container {
    position: absolute;
    width: clamp(115px, 18vw, 195px);
    height: clamp(115px, 18vw, 195px);
    z-index: 5;
}

.stick-outer {
    width: 100%; height: 100%;
    border-radius: 50%;
    background-color: var(--stick-outer);
    border: clamp(3px, 0.5vw, 6px) solid #444444;
    display: flex; align-items: center; justify-content: center;
    box-shadow: inset 0 4px 10px rgba(0,0,0,0.6);
    position: relative;
}

.stick-knob {
    width: 50%; height: 50%;
    border-radius: 50%;
    background-color: var(--knob-bg);
    border: clamp(3px, 0.5vw, 6px) solid #222222;
    box-shadow: inset 0 2px 4px rgba(255,255,255,0.2), 0 4px 8px rgba(0,0,0,0.6);
    position: absolute;
    transition: transform 0.04s linear;
}

/* ===== THEME SWITCHER BUTTON ===== */
.theme-switcher {
    position: absolute;
    top: clamp(4px, 1.5%, 12px);
    left: 50%;
    transform: translateX(-50%);
    display: flex;
    align-items: center;
    gap: 6px;
    padding: clamp(3px, 0.5vw, 6px) clamp(10px, 1.5vw, 18px);
    border-radius: 999px;
    background: rgba(60, 60, 60, 0.7);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    border: 1.5px solid rgba(255,255,255,0.12);
    color: #ccc;
    font-weight: 700;
    font-size: clamp(9px, 1.2vw, 13px);
    letter-spacing: 0.4px;
    cursor: pointer;
    z-index: 20;
    transition: background 0.15s ease, border-color 0.15s ease, transform 0.1s ease;
    white-space: nowrap;
}
.theme-switcher:active {
    transform: translateX(-50%) scale(0.93);
}
.theme-switcher svg {
    width: 14px; height: 14px;
    fill: currentColor;
    flex-shrink: 0;
}

/* ===== GLASS THEME ===== */
.theme-glass {
    --bg-color: #111111;
    --btn-fill: rgba(255, 255, 255, 0.08);
    --btn-border: rgba(255, 255, 255, 0.2);
    --btn-text: rgba(255, 255, 255, 0.7);
    --active-glow: #ffffff;
    --unlinked-glow: rgba(255, 255, 255, 0.3);
    --knob-bg: rgba(255, 255, 255, 0.12);
    --stick-outer: rgba(255, 255, 255, 0.05);
}
.theme-glass .controller-stage { background: linear-gradient(160deg, #111111 0%, #1a1a2e 50%, #111111 100%); }
.theme-glass .btn { backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px); box-shadow: 0 0 1px rgba(255,255,255,0.3), inset 0 1px 2px rgba(255,255,255,0.08); }
.theme-glass .btn:active, .theme-glass .btn.pressed { background-color: rgba(255,255,255,0.15); }
.theme-glass .dpad-btn { background-color: rgba(255, 255, 255, 0.06) !important; border-color: rgba(255,255,255,0.18) !important; backdrop-filter: blur(10px); }
.theme-glass .dpad-center { background-color: rgba(255,255,255,0.06) !important; border-color: rgba(255,255,255,0.15) !important; }
.theme-glass .stick-outer { border-color: rgba(255,255,255,0.15); background-color: rgba(255,255,255,0.04); backdrop-filter: blur(8px); }
.theme-glass .stick-knob { border-color: rgba(255,255,255,0.15); backdrop-filter: blur(6px); }
.theme-glass .guide-inner { background-color: rgba(255,255,255,0.06); color: rgba(255,255,255,0.5); }
.theme-glass .player-pill { background: rgba(255,255,255,0.06); border-color: rgba(255,255,255,0.15); backdrop-filter: blur(10px); }
.theme-glass .theme-switcher { background: rgba(255,255,255,0.06); border-color: rgba(255,255,255,0.15); backdrop-filter: blur(10px); }

/* CONTROLLER LAYOUTS */
.layout-ps4 .dpad { left: 6%; top: 18%; }
.layout-ps4 .left-stick { left: 26%; bottom: 10%; }
.layout-ps4 .right-stick { right: 26%; bottom: 10%; top: auto; }
.layout-ps4 .abxy-cluster { right: 6%; top: 18%; bottom: auto; }
.layout-ps4 .btn-lb { left: 6%; top: 3%; }
.layout-ps4 .btn-lt { left: 22%; top: 3%; }
.layout-ps4 .btn-rb { right: 6%; top: 3%; left: auto; }
.layout-ps4 .btn-rt { right: 22%; top: 3%; left: auto; }
.layout-ps4 .btn-ls { left: 16%; bottom: 18%; top: auto; }
.layout-ps4 .btn-rs { right: 16%; bottom: 18%; top: auto; left: auto; }
.layout-ps4 .btn-back { left: 38%; top: 14%; }
.layout-ps4 .btn-start { right: 38%; left: auto; top: 14%; }
.layout-ps4 .btn-guide { left: 50%; top: 80%; }
.layout-ps4 .player-pill { top: 16%; }

.layout-gameboy { --bg-color: #9bbc0f; --btn-fill: #306230; --btn-border: #0f380f; --btn-text: #9bbc0f; --knob-bg: #306230; --stick-outer: #8bac0f; }
.layout-gameboy .controller-stage { background: #9bbc0f; }
.layout-gameboy .btn { border-radius: 18%; }
.layout-gameboy .guide-inner { border-radius: 12%; background: #0f380f; color: #9bbc0f; }
.layout-gameboy .player-pill { background: #0f380f; border-color: #306230; color: #9bbc0f; top: 14%; }
.layout-gameboy .theme-switcher { background: #0f380f; border-color: #306230; color: #9bbc0f; }
.layout-gameboy .dpad { left: 10%; top: 50%; transform: translateY(-50%) scale(1.1); }
.layout-gameboy .left-stick, .layout-gameboy .right-stick { display: none; }
.layout-gameboy .abxy-cluster { right: 12%; top: 50%; bottom: auto; transform: translateY(-50%) rotate(-20deg); }
.layout-gameboy .btn-y, .layout-gameboy .btn-x { display: none; }
.layout-gameboy .btn-lb, .layout-gameboy .btn-lt, .layout-gameboy .btn-rb, .layout-gameboy .btn-rt, .layout-gameboy .btn-ls, .layout-gameboy .btn-rs { display: none; }
.layout-gameboy .btn-back { left: 38%; bottom: 12%; top: auto; }
.layout-gameboy .btn-start { left: 52%; right: auto; bottom: 12%; top: auto; }
.layout-gameboy .btn-guide { left: 50%; top: 32%; }

.layout-switch .left-stick { left: 8%; top: 16%; }
.layout-switch .dpad { left: 8%; bottom: 8%; top: auto; }
.layout-switch .abxy-cluster { right: 8%; top: 16%; bottom: auto; }
.layout-switch .right-stick { right: 8%; bottom: 8%; top: auto; }
.layout-switch .btn-lb { left: 6%; top: 3%; }
.layout-switch .btn-lt { left: 24%; top: 3%; }
.layout-switch .btn-rb { right: 6%; top: 3%; left: auto; }
.layout-switch .btn-rt { right: 24%; top: 3%; left: auto; }
.layout-switch .btn-ls { left: 26%; top: 22%; }
.layout-switch .btn-rs { right: 26%; top: 22%; left: auto; }
.layout-switch .btn-back { left: 36%; top: 16%; }
.layout-switch .btn-start { right: 36%; left: auto; top: 16%; }
.layout-switch .btn-guide { left: 50%; top: 48%; }

.layout-n64 .dpad { left: 6%; top: 28%; }
.layout-n64 .left-stick { left: 50%; bottom: 8%; top: auto; transform: translateX(-50%); }
.layout-n64 .abxy-cluster { right: 6%; top: 28%; bottom: auto; }
.layout-n64 .right-stick { right: 24%; top: 28%; bottom: auto; transform: scale(0.75); }
.layout-n64 .btn-lb { left: 6%; top: 4%; }
.layout-n64 .btn-rb { right: 6%; top: 4%; left: auto; }
.layout-n64 .btn-lt { left: 36%; bottom: 8%; top: auto; }
.layout-n64 .btn-rt { right: 24%; top: 4%; left: auto; }
.layout-n64 .btn-ls { left: 22%; top: 28%; }
.layout-n64 .btn-rs { right: 38%; top: 28%; left: auto; }
.layout-n64 .btn-start { left: 50%; top: 22%; transform: translateX(-50%); }
.layout-n64 .btn-back { left: 36%; top: 22%; }
.layout-n64 .btn-guide { left: 50%; top: 42%; }

.layout-steamdeck .left-stick { left: 6%; top: 14%; }
.layout-steamdeck .right-stick { right: 6%; top: 14%; }
.layout-steamdeck .dpad { left: 22%; top: 46%; }
.layout-steamdeck .abxy-cluster { right: 22%; top: 46%; bottom: auto; }
.layout-steamdeck .btn-lb { left: 6%; top: 2%; }
.layout-steamdeck .btn-lt { left: 20%; top: 2%; }
.layout-steamdeck .btn-rb { right: 6%; top: 2%; left: auto; }
.layout-steamdeck .btn-rt { right: 20%; top: 2%; left: auto; }
.layout-steamdeck .btn-ls { left: 6%; bottom: 10%; top: auto; }
.layout-steamdeck .btn-rs { right: 6%; bottom: 10%; top: auto; left: auto; }
.layout-steamdeck .btn-back { left: 36%; top: 14%; }
.layout-steamdeck .btn-start { right: 36%; left: auto; top: 14%; }
.layout-steamdeck .btn-guide { left: 50%; top: 52%; }

.layout-arcade .left-stick { left: 14%; top: 50%; bottom: auto; transform: translateY(-50%) scale(1.1); }
.layout-arcade .dpad { left: 5%; top: 10%; transform: scale(0.8); }
.layout-arcade .right-stick { display: none; }
.layout-arcade .abxy-cluster { right: 16%; top: 50%; bottom: auto; transform: translateY(-50%); }
.layout-arcade .btn-lb { right: 35%; top: 30%; left: auto; }
.layout-arcade .btn-lt { right: 35%; top: 62%; left: auto; }
.layout-arcade .btn-rb { right: 3%; top: 30%; left: auto; }
.layout-arcade .btn-rt { right: 3%; top: 62%; left: auto; }
.layout-arcade .btn-ls, .layout-arcade .btn-rs { display: none; }
.layout-arcade .btn-back { left: 36%; top: 10%; }
.layout-arcade .btn-start { right: 36%; left: auto; top: 10%; }
.layout-arcade .btn-guide { left: 50%; top: 12%; }
.layout-arcade .player-pill { top: 58%; }

.layout-hitbox .dpad { left: 8%; top: 50%; transform: translateY(-50%); }
.layout-hitbox .left-stick, .layout-hitbox .right-stick { display: none; }
.layout-hitbox .abxy-cluster { right: 14%; top: 50%; bottom: auto; transform: translateY(-50%); }
.layout-hitbox .btn-lb { right: 32%; top: 28%; left: auto; }
.layout-hitbox .btn-lt { right: 32%; top: 62%; left: auto; }
.layout-hitbox .btn-rb { right: 4%; top: 28%; left: auto; }
.layout-hitbox .btn-rt { right: 4%; top: 62%; left: auto; }
.layout-hitbox .btn-ls, .layout-hitbox .btn-rs { display: none; }
.layout-hitbox .btn-back { left: 38%; top: 12%; }
.layout-hitbox .btn-start { right: 38%; left: auto; top: 12%; }
.layout-hitbox .btn-guide { left: 50%; top: 14%; }

.layout-fightpad .dpad { left: 6%; top: 48%; transform: translateY(-50%); }
.layout-fightpad .left-stick { left: 24%; top: 48%; transform: translateY(-50%); }
.layout-fightpad .right-stick { display: none; }
.layout-fightpad .abxy-cluster { right: 14%; top: 48%; bottom: auto; transform: translateY(-50%); }
.layout-fightpad .btn-rb { right: 4%; top: 30%; left: auto; }
.layout-fightpad .btn-rt { right: 4%; top: 62%; left: auto; }
.layout-fightpad .btn-lb { left: 6%; top: 4%; }
.layout-fightpad .btn-lt { left: 24%; top: 4%; }
.layout-fightpad .btn-ls { left: 38%; top: 14%; }
.layout-fightpad .btn-rs { display: none; }
.layout-fightpad .btn-back { left: 38%; bottom: 12%; top: auto; }
.layout-fightpad .btn-start { right: 38%; left: auto; bottom: 12%; top: auto; }
.layout-fightpad .btn-guide { left: 50%; top: 18%; }

.layout-elite .left-stick { left: 8%; top: 18%; bottom: auto; }
.layout-elite .dpad { left: 22%; bottom: 8%; top: auto; }
.layout-elite .abxy-cluster { right: 8%; top: 18%; bottom: auto; }
.layout-elite .right-stick { right: 22%; bottom: 8%; top: auto; }
.layout-elite .btn-lb { left: 6%; top: 3%; }
.layout-elite .btn-lt { left: 22%; top: 3%; }
.layout-elite .btn-rb { right: 6%; top: 3%; left: auto; }
.layout-elite .btn-rt { right: 22%; top: 3%; left: auto; }
.layout-elite .btn-ls { left: 6%; bottom: 10%; top: auto; }
.layout-elite .btn-rs { right: 6%; bottom: 10%; top: auto; left: auto; }
.layout-elite .btn-back { left: 38%; top: 16%; }
.layout-elite .btn-start { right: 38%; left: auto; top: 16%; }
.layout-elite .btn-guide { left: 50%; top: 18%; }
.layout-elite .player-pill { top: 58%; }

.layout-fpspro .left-stick { left: 22%; top: 28%; transform: scale(1.1); }
.layout-fpspro .right-stick { right: 22%; top: 28%; transform: scale(1.1); }
.layout-fpspro .dpad { left: 5%; bottom: 8%; top: auto; transform: scale(0.9); }
.layout-fpspro .abxy-cluster { right: 5%; bottom: 8%; top: auto; transform: scale(0.9); }
.layout-fpspro .btn-ls { left: 8%; top: 32%; }
.layout-fpspro .btn-rs { right: 8%; top: 32%; left: auto; }
.layout-fpspro .btn-lb { left: 5%; top: 3%; }
.layout-fpspro .btn-lt { left: 22%; top: 3%; }
.layout-fpspro .btn-rb { right: 5%; top: 3%; left: auto; }
.layout-fpspro .btn-rt { right: 22%; top: 3%; left: auto; }
.layout-fpspro .btn-back { left: 38%; top: 14%; }
.layout-fpspro .btn-start { right: 38%; left: auto; top: 14%; }
.layout-fpspro .btn-guide { left: 50%; top: 48%; }

.layout-3ds .left-stick { left: 6%; top: 16%; }
.layout-3ds .dpad { left: 6%; bottom: 8%; top: auto; transform: scale(0.9); }
.layout-3ds .abxy-cluster { right: 6%; top: 16%; bottom: auto; }
.layout-3ds .right-stick { right: 8%; bottom: 10%; top: auto; transform: scale(0.75); }
.layout-3ds .btn-lb { left: 6%; top: 3%; }
.layout-3ds .btn-lt { left: 22%; top: 3%; }
.layout-3ds .btn-rb { right: 6%; top: 3%; left: auto; }
.layout-3ds .btn-rt { right: 22%; top: 3%; left: auto; }
.layout-3ds .btn-ls { left: 24%; top: 22%; }
.layout-3ds .btn-rs { right: 24%; bottom: 26%; top: auto; left: auto; }
.layout-3ds .btn-back { left: 36%; bottom: 10%; top: auto; }
.layout-3ds .btn-start { right: 36%; left: auto; bottom: 10%; top: auto; }
.layout-3ds .btn-guide { left: 50%; top: 45%; }

.layout-gamecube .left-stick { left: 8%; top: 18%; }
.layout-gamecube .dpad { left: 10%; bottom: 8%; top: auto; transform: scale(0.85); }
.layout-gamecube .abxy-cluster { right: 8%; top: 18%; bottom: auto; }
.layout-gamecube .btn-a { width: 48%; height: 48%; top: 26%; left: 26%; background-color: #2e8b57; color: #fff; }
.layout-gamecube .btn-b { width: 28%; height: 28%; top: 40%; left: 0%; background-color: #cd5c5c; color: #fff; }
.layout-gamecube .btn-x { width: 35%; height: 26%; top: 5%; left: 50%; border-radius: 12px; }
.layout-gamecube .btn-y { width: 26%; height: 35%; top: 30%; left: 75%; border-radius: 12px; }
.layout-gamecube .right-stick { right: 22%; bottom: 10%; top: auto; transform: scale(0.8); }
.layout-gamecube .right-stick .stick-knob { background-color: #e6b800; }
.layout-gamecube .btn-lb { left: 6%; top: 3%; }
.layout-gamecube .btn-lt { left: 22%; top: 3%; }
.layout-gamecube .btn-rb { right: 6%; top: 3%; left: auto; background-color: #a020f0; color: #fff; }
.layout-gamecube .btn-rt { right: 22%; top: 3%; left: auto; }
.layout-gamecube .btn-ls { left: 24%; top: 22%; }
.layout-gamecube .btn-rs { right: 38%; bottom: 14%; top: auto; left: auto; }
.layout-gamecube .btn-back { left: 38%; top: 14%; }
.layout-gamecube .btn-start { left: 50%; top: 45%; transform: translateX(-50%); }
.layout-gamecube .btn-guide { left: 50%; top: 18%; }

.layout-saturn .dpad { left: 8%; top: 48%; transform: translateY(-50%) scale(1.1); }
.layout-saturn .left-stick { left: 26%; top: 48%; transform: translateY(-50%) scale(0.9); }
.layout-saturn .right-stick { display: none; }
.layout-saturn .abxy-cluster { right: 16%; top: 48%; bottom: auto; transform: translateY(-50%); }
.layout-saturn .btn-rb { right: 4%; top: 32%; left: auto; }
.layout-saturn .btn-rt { right: 4%; top: 64%; left: auto; }
.layout-saturn .btn-lb { left: 6%; top: 3%; }
.layout-saturn .btn-lt { left: 24%; top: 3%; }
.layout-saturn .btn-ls { left: 40%; top: 14%; }
.layout-saturn .btn-rs { display: none; }
.layout-saturn .btn-back { left: 38%; bottom: 12%; top: auto; }
.layout-saturn .btn-start { right: 38%; left: auto; bottom: 12%; top: auto; }
.layout-saturn .btn-guide { left: 50%; top: 18%; }

.layout-cyberclaw .left-stick { left: 14%; top: 45%; transform: translateY(-50%); }
.layout-cyberclaw .right-stick { right: 14%; top: 45%; transform: translateY(-50%); }
.layout-cyberclaw .dpad { left: 4%; top: 12%; transform: scale(0.85); }
.layout-cyberclaw .abxy-cluster { right: 4%; top: 12%; bottom: auto; transform: scale(0.85); }
.layout-cyberclaw .btn-lb { left: 28%; top: 4%; }
.layout-cyberclaw .btn-lt { left: 40%; top: 4%; }
.layout-cyberclaw .btn-rb { right: 28%; top: 4%; left: auto; }
.layout-cyberclaw .btn-rt { right: 40%; top: 4%; left: auto; }
.layout-cyberclaw .btn-ls { left: 4%; bottom: 10%; top: auto; }
.layout-cyberclaw .btn-rs { right: 4%; bottom: 10%; top: auto; left: auto; }
.layout-cyberclaw .btn-back { left: 38%; bottom: 10%; top: auto; }
.layout-cyberclaw .btn-start { right: 38%; bottom: 10%; top: auto; left: auto; }
.layout-cyberclaw .btn-guide { left: 50%; top: 48%; }

.layout-onehand .left-stick { left: 50%; bottom: 6%; top: auto; transform: translateX(-50%) scale(1.15); }
.layout-onehand .dpad { left: 8%; top: 38%; transform: scale(0.95); }
.layout-onehand .abxy-cluster { right: 8%; top: 38%; bottom: auto; transform: scale(0.95); }
.layout-onehand .right-stick { display: none; }
.layout-onehand .btn-lb { left: 6%; top: 4%; }
.layout-onehand .btn-lt { left: 24%; top: 4%; }
.layout-onehand .btn-rb { right: 6%; top: 4%; left: auto; }
.layout-onehand .btn-rt { right: 24%; top: 4%; left: auto; }
.layout-onehand .btn-ls { left: 26%; bottom: 12%; top: auto; }
.layout-onehand .btn-rs { right: 26%; bottom: 12%; top: auto; left: auto; }
.layout-onehand .btn-back { left: 38%; top: 16%; }
.layout-onehand .btn-start { right: 38%; top: 16%; left: auto; }
.layout-onehand .btn-guide { left: 50%; top: 22%; }
</style>
</head>
<body>
<div class="controller-stage" id="controllerStage">
    <!-- THEME SWITCHER -->
    <div id="themeSwitcher" class="theme-switcher" title="Switch Controller Layout">
        <svg viewBox="0 0 24 24"><path d="M12 22C6.49 22 2 17.51 2 12S6.49 2 12 2s10 4.04 10 9c0 3.31-2.69 6-6 6h-1.77c-.28 0-.5.22-.5.5 0 .12.05.23.13.33.41.47.64 1.06.64 1.67A2.5 2.5 0 0 1 12 22zm0-18c-4.41 0-8 3.59-8 8s3.59 8 8 8c.28 0 .5-.22.5-.5a.54.54 0 0 0-.14-.35c-.41-.46-.63-1.05-.63-1.65a2.5 2.5 0 0 1 2.5-2.5H16c2.21 0 4-1.79 4-4 0-3.86-3.59-7-8-7z"/><circle cx="6.5" cy="11.5" r="1.5"/><circle cx="9.5" cy="7.5" r="1.5"/><circle cx="14.5" cy="7.5" r="1.5"/><circle cx="17.5" cy="11.5" r="1.5"/></svg>
        <span id="themeLabel">Xbox Default</span>
    </div>

    <!-- LEFT SIDE -->
    <button class="btn btn-round btn-ls" data-btn="LS" aria-label="LS">LS</button>

    <div class="dpad">
        <button class="dpad-btn dpad-up" data-btn="DPAD_UP" aria-label="Up"></button>
        <button class="dpad-btn dpad-left" data-btn="DPAD_LEFT" aria-label="Left"></button>
        <div class="dpad-center" aria-hidden="true"></div>
        <button class="dpad-btn dpad-right" data-btn="DPAD_RIGHT" aria-label="Right"></button>
        <button class="dpad-btn dpad-down" data-btn="DPAD_DOWN" aria-label="Down"></button>
    </div>

    <div class="stick-container left-stick" data-joystick="left">
        <div class="stick-outer">
            <div class="stick-knob"></div>
        </div>
    </div>

    <button class="btn btn-round btn-back" data-btn="BACK" aria-label="Back">
        <svg viewBox="0 0 24 24" width="55%" height="55%" fill="currentColor"><path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/></svg>
    </button>

    <button class="btn btn-round btn-lb" data-btn="LB" aria-label="LB">LB</button>
    <button class="btn btn-round btn-lt" data-btn="LT" aria-label="LT">LT</button>

    <!-- CENTER -->
    <button class="btn btn-guide" data-btn="GUIDE" aria-label="Guide">
        <div class="guide-inner">
            <svg viewBox="0 0 24 24" width="60%" height="60%" fill="none" stroke="currentColor" stroke-width="2.2">
                <circle cx="12" cy="12" r="9"/>
                <path d="M9 16c2-1.5 4-1.5 6 0" stroke-linecap="round"/>
                <path d="M8 12c2.5-2 5.5-2 8 0" stroke-linecap="round"/>
            </svg>
        </div>
    </button>

    <div id="playerPill" class="player-pill" title="Click to cycle Gyroscope Mode">
        <span class="status-indicator"></span>
        <span id="playerLabel">Unlinked</span>
    </div>

    <button class="btn btn-round btn-start" data-btn="START" aria-label="Start">
        <svg viewBox="0 0 24 24" width="55%" height="55%" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>
    </button>

    <!-- RIGHT SIDE -->
    <button class="btn btn-round btn-rb" data-btn="RB" aria-label="RB">RB</button>
    <button class="btn btn-round btn-rt" data-btn="RT" aria-label="RT">RT</button>

    <div class="stick-container right-stick" data-joystick="right">
        <div class="stick-outer">
            <div class="stick-knob"></div>
        </div>
    </div>

    <button class="btn btn-round btn-rs" data-btn="RS" aria-label="RS">RS</button>

    <div class="abxy-cluster">
        <button class="btn btn-round btn-abxy btn-y" data-btn="Y">Y</button>
        <button class="btn btn-round btn-abxy btn-x" data-btn="X">X</button>
        <button class="btn btn-round btn-abxy btn-b" data-btn="B">B</button>
        <button class="btn btn-round btn-abxy btn-a" data-btn="A">A</button>
    </div>
</div>

<script>
(() => {
    const state = {
        ws: null,
        connected: false,
        playerId: null,
        joystickActive: new Map(),
        pingTimer: null,
        packetSeq: 0,
        buttonStateMask: 0,
        leftTrigger: 0,
        rightTrigger: 0,
        stickState: { lx: 0, ly: 0, rx: 0, ry: 0 },
        gyroModeIndex: 0,
        gyroCalibratedGamma: null,
        gyroCalibratedBeta: null,
        gyroEnabled: false,
        gyroReady: false,
        gyroLastTime: 0,
        gyroSmoothX: 0,
        gyroSmoothY: 0,
        turboIntervals: new Map(),
        layoutIndex: 0,
        gyroSensorSource: "none",
        gyroSensorSeen: false,
        sendScheduled: false,
        stateDirty: false,
        lastSentAt: 0,
        lastPacketTime: 0,
        reconnectDelay: 250,
        reconnectTimer: null,
        lastServerPong: 0,
        consecutiveSends: 0,
        lastInputAt: performance.now(),
        serverLatencyMs: 0,
        packetLossEstimate: 0,
        inputHz: 120,
        lastWelcomeAt: 0,
        connectionEpoch: 0,
    };

    const LAYOUTS = [
        { name: "Xbox Default", className: null },
        { name: "Glass", className: "theme-glass" },
        { name: "PS4", className: "layout-ps4" },
        { name: "Game Boy", className: "layout-gameboy" },
        { name: "Switch", className: "layout-switch" },
        { name: "N64", className: "layout-n64" },
        { name: "Steam Deck", className: "layout-steamdeck" },
        { name: "Arcade", className: "layout-arcade" },
        { name: "Hitbox", className: "layout-hitbox" },
        { name: "Fight Pad", className: "layout-fightpad" },
        { name: "Xbox Elite Pro", className: "layout-elite" },
        { name: "FPS Pro", className: "layout-fpspro" },
        { name: "3DS Dual", className: "layout-3ds" },
        { name: "GameCube", className: "layout-gamecube" },
        { name: "Sega Saturn", className: "layout-saturn" },
        { name: "Cyber Claw", className: "layout-cyberclaw" },
        { name: "One-Handed", className: "layout-onehand" },
    ];

    function applyLayout(index) {
        const stage = document.getElementById("controllerStage");

        for (const layout of LAYOUTS) {
            if (layout.className) stage.classList.remove(layout.className);
        }

        state.layoutIndex = index;
        const layout = LAYOUTS[state.layoutIndex];

        if (layout.className) stage.classList.add(layout.className);

        document.getElementById("themeLabel").textContent = layout.name;

        try { localStorage.setItem("touch-layout", String(state.layoutIndex)); } catch(e) {}
    }

    function cycleLayout() {
        const next = (state.layoutIndex + 1) % LAYOUTS.length;
        applyLayout(next);
        Haptics.playDualRumble(0.3, 0.3, 40);
    }

    function restoreLayout() {
        try {
            const saved = parseInt(localStorage.getItem("touch-layout") || "0", 10);
            if (Number.isInteger(saved) && saved >= 0 && saved < LAYOUTS.length) {
                applyLayout(saved);
                return;
            }
        } catch(e) {}
        applyLayout(0);
    }

    document.getElementById("themeSwitcher").addEventListener("click", e => {
        e.preventDefault();
        e.stopPropagation();
        cycleLayout();
    });

    restoreLayout();

    const $ = (s, root = document) => root.querySelector(s);
    const $$ = (s, root = document) => [...root.querySelectorAll(s)];

    // === ADD THIS RIGHT AFTER const state = { ... } ===
let vibrationUnlocked = false;
const pendingRumble = { weak: 0, strong: 0, duration: 0, active: false };
let activeGameRumble = { weak: 0, strong: 0 };
let rumbleLoopTimer = null;
let rumbleGeneration = 0;
const RUMBLE_TICK_MS = 45;

function stopGameRumble() {
    activeGameRumble.weak = 0;
    activeGameRumble.strong = 0;
    rumbleGeneration++;
    if (rumbleLoopTimer) {
        clearInterval(rumbleLoopTimer);
        rumbleLoopTimer = null;
    }
    try { if ("vibrate" in navigator) navigator.vibrate(0); } catch (_) {}
}

function startGameRumble(weak, strong) {
    weak = Math.max(0, Math.min(1, weak));
    strong = Math.max(0, Math.min(1, strong));
    activeGameRumble.weak = weak;
    activeGameRumble.strong = strong;
    const generation = ++rumbleGeneration;
    if (rumbleLoopTimer) clearInterval(rumbleLoopTimer);
    const tick = () => {
        if (generation !== rumbleGeneration) return;
        const w = activeGameRumble.weak;
        const st = activeGameRumble.strong;
        if (w <= 0 && st <= 0) { stopGameRumble(); return; }
        Haptics.playDualRumble(w, st, RUMBLE_TICK_MS + 25, true);
    };
    tick();
    rumbleLoopTimer = setInterval(tick, RUMBLE_TICK_MS);
}

const unlockVibration = () => {
    if (vibrationUnlocked) return;
    vibrationUnlocked = true;
    // If a rumble message arrived before unlock, fire it now
    if (pendingRumble.active) {
        Haptics.playDualRumble(pendingRumble.weak, pendingRumble.strong, pendingRumble.duration);
        pendingRumble.active = false;
    }
};
window.addEventListener('pointerdown', unlockVibration, { passive: true, once: true });
window.addEventListener('touchstart', unlockVibration, { passive: true, once: true });
// === END ADD ===


// === REPLACE THE ENTIRE Haptics OBJECT ===
let lastHapticAt = 0;
const Haptics = {
    playDualRumble(weakMagnitude = 0.5, strongMagnitude = 0.5, duration = 120, persistent = false) {
        const now = performance.now();
        if (!persistent && now - lastHapticAt < 18 && duration < 100) return;
        lastHapticAt = now;
        const intensity = Math.max(weakMagnitude, strongMagnitude);

        // 1. Physical gamepad paired to phone (rare, but try it)
        try {
            const gamepads = navigator.getGamepads?.() || [];
            for (let gp of gamepads) {
                if (gp?.vibrationActuator?.playEffect) {
                    gp.vibrationActuator.playEffect("dual-rumble", {
                        startDelay: 0,
                        duration,
                        weakMagnitude,
                        strongMagnitude,
                    }).catch(() => {});
                }
            }
        } catch (e) {}

        // 2. Phone vibration fallback
        if (!("vibrate" in navigator) || intensity <= 0) return;

        // If API isn't unlocked yet, queue it for the first touch
        if (!vibrationUnlocked) {
            pendingRumble.weak = weakMagnitude;
            pendingRumble.strong = strongMagnitude;
            pendingRumble.duration = duration;
            pendingRumble.active = true;
            return;
        }

        try {
            // 40ms minimum — shorter pulses often don't spin the motor
            const pulse = Math.max(40, Math.min(Math.round(duration * intensity), 250));
            // Double-pulse for strong rumble so the motor actually kicks in
            const pattern = persistent ? [pulse] : (intensity > 0.55 ? [pulse, 30, pulse] : [pulse]);
            navigator.vibrate(pattern);
        } catch (e) {}
    },

    vibrate(pattern = 20) {
        if (!vibrationUnlocked || !("vibrate" in navigator)) return;
        try { navigator.vibrate(pattern); } catch (e) {}
    },

    fifaPass() { this.playDualRumble(0.2, 0.4, 30); },
    fifaThroughBall() { this.playDualRumble(0.3, 0.6, 45); },
    fifaShot() { this.playDualRumble(0.8, 1.0, 75); },
    fifaTackle() { this.playDualRumble(1.0, 0.8, 85); },
    forzaEngineRumble(throttleVal = 255) {
        const intensity = (throttleVal / 255) * 0.4;
        this.playDualRumble(intensity, intensity * 0.5, 35);
    },
    forzaBrakeABS() { this.playDualRumble(0.9, 0.3, 50); },
    forzaTireSlip() { this.playDualRumble(0.6, 0.8, 40); }
};


    const BTN_MASKS = {
        "DPAD_UP": 0x0001, "DPAD_DOWN": 0x0002, "DPAD_LEFT": 0x0004, "DPAD_RIGHT": 0x0008,
        "START": 0x0010, "BACK": 0x0020, "LS": 0x0040, "RS": 0x0080,
        "LB": 0x0100, "RB": 0x0200, "GUIDE": 0x0400,
        "A": 0x1000, "B": 0x2000, "X": 0x4000, "Y": 0x8000
    };

    let wakeLock = null;
    async function requestWakeLock() {
        if ("wakeLock" in navigator && !wakeLock) {
            try { wakeLock = await navigator.wakeLock.request("screen"); } catch (e) {}
        }
    }

    function isLandscape() {
        if (screen.orientation && screen.orientation.type) {
            return screen.orientation.type.startsWith("landscape");
        }
        return window.innerWidth > window.innerHeight;
    }

    function requestFullscreenMode() {
        const el = document.documentElement;
        if (document.fullscreenElement || document.webkitFullscreenElement) return;
        const req = el.requestFullscreen || el.webkitRequestFullscreen || el.mozRequestFullScreen || el.msRequestFullscreen;
        req?.call(el).catch(() => {});
    }

    function exitFullscreenMode() {
        if (document.fullscreenElement || document.webkitFullscreenElement) {
            const exit = document.exitFullscreen || document.webkitExitFullscreen || document.mozCancelFullScreen || document.msExitFullscreen;
            exit?.call(document).catch(() => {});
        }
    }

    function handleOrientationChange() {
        if (isLandscape()) requestFullscreenMode();
        else exitFullscreenMode();
    }

    if (screen.orientation) {
        screen.orientation.addEventListener("change", handleOrientationChange);
    }
    window.addEventListener("resize", handleOrientationChange);
    window.addEventListener("orientationchange", handleOrientationChange);
    window.visualViewport?.addEventListener("resize", positionPlayerPill);
    window.visualViewport?.addEventListener("scroll", positionPlayerPill);

    window.addEventListener("pointerdown", () => {
        requestWakeLock();
        if (isLandscape()) requestFullscreenMode();
    }, { passive: true });

    positionPlayerPill();
    requestAnimationFrame(positionPlayerPill);

    function updatePillLabel() {
    const pill = $("#playerPill");
    const label = $("#playerLabel");

    if (!state.connected || !state.playerId) {
        pill.classList.remove("connected");
        label.textContent = "—";
        positionPlayerPill();
        return;
    }

    pill.classList.add("connected");

    // Server-assigned ID is the actual permanent ViGEm slot.
    label.textContent = String(Number(state.playerId));

    positionPlayerPill();
}

function rectsOverlap(a, b, gap = 6) {
    return !(
        a.right + gap <= b.left ||
        a.left - gap >= b.right ||
        a.bottom + gap <= b.top ||
        a.top - gap >= b.bottom
    );
}

function positionPlayerPill() {
    const pill = $("#playerPill");
    if (!pill) return;

    pill.style.left = "auto";
    pill.style.right = "auto";
    pill.style.top = "auto";
    pill.style.bottom = "auto";
    pill.classList.remove("player-collision-safe");

    const margin = Math.max(8, Math.round(window.innerWidth * 0.015));
    const viewportTop = (window.visualViewport && window.visualViewport.offsetTop) || 0;
    const safeTop = Math.max(8, viewportTop) + margin;
    const w = pill.offsetWidth;
    const h = pill.offsetHeight;

    // Prefer the top-right, then top-left, then top-center.
    // Extra top positions handle layouts with shoulder buttons.
    const candidates = [
        { left: window.innerWidth - w - margin, top: safeTop },
        { left: margin, top: safeTop },
        { left: Math.round((window.innerWidth - w) / 2), top: safeTop },
        { left: window.innerWidth - w - margin, top: safeTop + h + 8 },
        { left: margin, top: safeTop + h + 8 }
    ];

    const obstacles = [
        ...document.querySelectorAll(".btn, .dpad, .stick-container, .abxy-cluster, #themeSwitcher")
    ].filter(el => el !== pill && getComputedStyle(el).display !== "none");

    for (const candidate of candidates) {
        const testRect = {
            left: candidate.left,
            right: candidate.left + w,
            top: candidate.top,
            bottom: candidate.top + h
        };

        const collision = obstacles.some(el =>
            rectsOverlap(testRect, el.getBoundingClientRect())
        );

        if (!collision) {
            pill.style.left = `${candidate.left}px`;
            pill.style.top = `${candidate.top}px`;
            pill.classList.add("player-collision-safe");
            return;
        }
    }

    // Last resort: keep it visible at the top-right.
    pill.style.left = `${window.innerWidth - w - margin}px`;
    pill.style.top = `${safeTop}px`;
    pill.classList.add("player-collision-safe");
}

function setConnection(connected, playerId = null) {
        if (!connected) stopGameRumble();
        state.connected = connected;
        if (playerId !== null && playerId !== undefined) state.playerId = Number(playerId);
        if (!connected) state.playerId = null;
        if (connected) Haptics.playDualRumble(0.5, 0.5, 60);
        updatePillLabel();
    }

    let gyroListenersInstalled = false;

    function installGyroListeners() {
        if (gyroListenersInstalled) return;
        gyroListenersInstalled = true;

        window.addEventListener("deviceorientation", handleOrientation, { passive: true });
        window.addEventListener("deviceorientationabsolute", handleOrientation, { passive: true });
        window.addEventListener("devicemotion", handleMotion, { passive: true });
    }

    async function requestGyroPermission() {
        try {
            // iOS/Safari exposes requestPermission(); most Android browsers
            // don't. On Android, the important requirement is a secure context.
            if (typeof DeviceOrientationEvent !== "undefined" &&
                typeof DeviceOrientationEvent.requestPermission === "function") {
                const result = await DeviceOrientationEvent.requestPermission();
                if (result !== "granted") return false;
            }

            if (typeof DeviceMotionEvent !== "undefined" &&
                typeof DeviceMotionEvent.requestPermission === "function") {
                const result = await DeviceMotionEvent.requestPermission();
                if (result !== "granted") return false;
            }

            return true;
        } catch (e) {
            console.warn("[gyro] permission request failed:", e);
            return false;
        }
    }

    function resetGyroCalibration() {
        state.gyroCalibratedGamma = null;
        state.gyroCalibratedBeta = null;
        state.gyroSmoothX = 0;
        state.gyroSmoothY = 0;
        state.gyroLastTime = 0;
        state.gyroSensorSource = "none";
        state.gyroSensorSeen = false;
    }

    async function enableGyro() {
        // DeviceOrientation/DeviceMotion are secure-context APIs in modern
        // browsers. The current server is HTTP, so Android Chrome will block
        // the sensors when opened using the LAN http:// address.
        if (!window.isSecureContext && location.hostname !== "localhost" &&
            location.hostname !== "127.0.0.1") {
            state.gyroReady = false;
            state.gyroEnabled = false;
            console.warn("[gyro] HTTPS is required on Android for sensor access.");
            return false;
        }

        const hasOrientation = typeof DeviceOrientationEvent !== "undefined";
        const hasMotion = typeof DeviceMotionEvent !== "undefined";

        if (!hasOrientation && !hasMotion) {
            state.gyroReady = false;
            state.gyroEnabled = false;
            return false;
        }

        const permissionOk = await requestGyroPermission();
        if (!permissionOk) {
            state.gyroReady = false;
            state.gyroEnabled = false;
            return false;
        }

        installGyroListeners();
        state.gyroReady = true;
        state.gyroEnabled = true;
        resetGyroCalibration();
        return true;
    }

    async function cycleGyroMode() {
        state.gyroModeIndex = (state.gyroModeIndex + 1) % 3;

        if (state.gyroModeIndex !== 0) {
            const ok = await enableGyro();
            if (!ok) {
                // Don't leave the UI saying gyro is active when the browser
                // rejected the sensor.
                state.gyroModeIndex = 0;
            }
        } else {
            state.gyroReady = false;
            state.gyroEnabled = false;
        }

        resetGyroCalibration();

        if (state.gyroModeIndex === 1) {
            state.stickState.lx = 0;
            state.stickState.ly = 0;
            updateKnobVisual($(".left-stick"), 0, 0);
        } else if (state.gyroModeIndex === 2) {
            state.stickState.rx = 0;
            state.stickState.ry = 0;
            updateKnobVisual($(".right-stick"), 0, 0);
        } else {
            state.stickState.lx = 0;
            state.stickState.ly = 0;
            state.stickState.rx = 0;
            state.stickState.ry = 0;
            updateKnobVisual($(".left-stick"), 0, 0);
            updateKnobVisual($(".right-stick"), 0, 0);
        }

        updatePillLabel();
        sendBinaryState();
    }

    const playerPill = $("#playerPill");
    const switchGyroFromPlayerButton = e => {
        e.preventDefault();
        e.stopPropagation();

        cycleGyroMode();
    };

    playerPill.addEventListener("pointerup", switchGyroFromPlayerButton);
    playerPill.style.cursor = "pointer";
    playerPill.style.touchAction = "manipulation";

    function applyGyro(x, y, source) {
        if (state.gyroModeIndex === 0 || !state.gyroReady) return;

        state.gyroSensorSeen = true;
        state.gyroSensorSource = source;

        if (state.gyroCalibratedGamma === null ||
            state.gyroCalibratedBeta === null) {
            state.gyroCalibratedGamma = x;
            state.gyroCalibratedBeta = y;
            state.gyroSmoothX = 0;
            state.gyroSmoothY = 0;
            return;
        }

        // Compensate for phone rotation so landscape-left/right behaves consistently.
        const angle = ((screen.orientation?.angle ?? window.orientation ?? 0) + 360) % 360;
        let gx = x - state.gyroCalibratedGamma;
        let gy = y - state.gyroCalibratedBeta;

        if (angle === 90) {
            [gx, gy] = [gy, -gx];
        } else if (angle === 180) {
            gx = -gx; gy = -gy;
        } else if (angle === 270) {
            [gx, gy] = [-gy, gx];
        }

        const dead = 1.2;
        if (Math.abs(gx) < dead) gx = 0;
        if (Math.abs(gy) < dead) gy = 0;

        let targetX = Math.max(-1, Math.min(1, gx / 22));
        let targetY = Math.max(-1, Math.min(1, gy / 22));

        // Smooth-step curve: fine near center, aggressive near the edge.
        targetX = Math.sign(targetX) * Math.abs(targetX) * Math.abs(targetX) * (3 - 2 * Math.abs(targetX));
        targetY = Math.sign(targetY) * Math.abs(targetY) * Math.abs(targetY) * (3 - 2 * Math.abs(targetY));

        const gyroMagnitude = Math.max(Math.abs(targetX), Math.abs(targetY));
        const alpha = gyroMagnitude > 0.65 ? 0.52 : gyroMagnitude > 0.25 ? 0.36 : 0.22;
        state.gyroSmoothX += (targetX - state.gyroSmoothX) * alpha;
        state.gyroSmoothY += (targetY - state.gyroSmoothY) * alpha;

        const sx = Math.max(-1, Math.min(1, state.gyroSmoothX));
        const sy = Math.max(-1, Math.min(1, state.gyroSmoothY));

        if (state.gyroModeIndex === 1) {
            state.stickState.lx = sx;
            state.stickState.ly = sy;
            updateKnobVisual($(".left-stick"), sx, sy);
        } else {
            state.stickState.rx = sx;
            state.stickState.ry = sy;
            updateKnobVisual($(".right-stick"), sx, sy);
        }

        const now = performance.now();
        if (now - state.gyroLastTime >= 12) {
            state.gyroLastTime = now;
            sendBinaryState();
        }
    }

    function handleOrientation(e) {
        if (Number.isFinite(e.gamma) && Number.isFinite(e.beta)) {
            applyGyro(e.gamma, e.beta, "orientation");
        }
    }

    function handleMotion(e) {
        const r = e.rotationRate;
        if (!r || !Number.isFinite(r.alpha) ||
            !Number.isFinite(r.beta) || !Number.isFinite(r.gamma)) return;

        // Only use motion as a fallback when orientation data has not arrived.
        // rotationRate is angular velocity, not an absolute stick position.
        if (state.gyroSensorSeen && state.gyroSensorSource === "orientation") {
            return;
        }

        if (state.gyroModeIndex === 0 || !state.gyroReady) return;

        state.gyroSensorSeen = true;
        state.gyroSensorSource = "motion";

        const dx = Math.max(-1, Math.min(1, r.gamma / 90));
        const dy = Math.max(-1, Math.min(1, r.beta / 90));

        state.gyroSmoothX += (dx - state.gyroSmoothX) * 0.16;
        state.gyroSmoothY += (dy - state.gyroSmoothY) * 0.16;

        if (state.gyroModeIndex === 1) {
            state.stickState.lx = state.gyroSmoothX;
            state.stickState.ly = state.gyroSmoothY;
            updateKnobVisual($(".left-stick"), state.gyroSmoothX, state.gyroSmoothY);
        } else {
            state.stickState.rx = state.gyroSmoothX;
            state.stickState.ry = state.gyroSmoothY;
            updateKnobVisual($(".right-stick"), state.gyroSmoothX, state.gyroSmoothY);
        }

        sendBinaryState();
    }

    function updateKnobVisual(container, x, y) {
        if (!container) return;
        const knob = $(".stick-knob", container);
        if (!knob) return;

        const maxTranslate = Math.max(
            0,
            Math.min(container.clientWidth, container.clientHeight) * 0.38
        );
        knob.style.transform = `translate3d(${x * maxTranslate}px, ${y * maxTranslate}px, 0)`;
    }

    function startPing() {
        stopPing();
        state.pingTimer = setInterval(() => {
            if (state.ws && state.ws.readyState === WebSocket.OPEN) {
                state.lastServerPongSent = performance.now();
                state.ws.send(JSON.stringify({ type: "ping" }));
            }
            if (state.lastServerPong && performance.now() - state.lastServerPong > 7000) {
                try { state.ws.close(); } catch (_) {}
            }
        }, 2000);
    }

    function stopPing() {
        if (state.pingTimer) {
            clearInterval(state.pingTimer);
            state.pingTimer = null;
        }
    }

    function sendBinaryState(immediate = false) {
        if (!state.ws || state.ws.readyState !== WebSocket.OPEN || !state.playerId) return;

        state.stateDirty = true;
        state.lastInputAt = performance.now();
        const now = performance.now();
        const minInterval = 1000 / Math.max(60, Math.min(240, state.inputHz || 120));

        if (!immediate && (now - state.lastSentAt) < minInterval) {
            if (!state.sendScheduled) {
                state.sendScheduled = true;
                setTimeout(() => {
                    state.sendScheduled = false;
                    sendBinaryState(true);
                }, Math.max(0, minInterval - (now - state.lastSentAt)));
            }
            return;
        }

        if (!state.stateDirty) return;
        state.stateDirty = false;
        state.lastSentAt = now;
        state.packetSeq = (state.packetSeq + 1) & 0xFFFF;

            const buffer = new ArrayBuffer(12);
        const view = new DataView(buffer);

        view.setUint8(0, state.playerId);
        view.setUint16(1, state.packetSeq);
        view.setUint8(3, 1);
        view.setUint16(4, state.buttonStateMask);
        view.setUint8(6, state.leftTrigger);
        view.setUint8(7, state.rightTrigger);
        view.setInt8(8, Math.round(state.stickState.lx * 127));
        view.setInt8(9, Math.round(state.stickState.ly * 127));
        view.setInt8(10, Math.round(state.stickState.rx * 127));
        view.setInt8(11, Math.round(state.stickState.ry * 127));

        try { state.ws.send(buffer); } catch (_) {}
    }

    function connect() {
        try {
            const protocol = location.protocol === "https:" ? "wss:" : "ws:";
            state.ws = new WebSocket(`${protocol}//${location.host}/ws`);
            state.ws.binaryType = "arraybuffer";

            state.ws.addEventListener("open", () => {
                state.connectionEpoch++;
                state.reconnectDelay = 250;
                state.lastServerPong = performance.now();
                // Do not mark the controller assigned until the server sends
                // the authoritative welcome/player slot.
                startPing();
            });

            state.ws.addEventListener("close", () => {
                setConnection(false);
                stopPing();
                if (state.reconnectTimer) clearTimeout(state.reconnectTimer);
                const jitter = Math.random() * 150;
                const delay = Math.min(state.reconnectDelay + jitter, 5000);
                state.reconnectDelay = Math.min(state.reconnectDelay * 1.7, 5000);
                state.reconnectTimer = setTimeout(() => { state.reconnectTimer = null; connect(); }, delay);
            });

            state.ws.addEventListener("error", () => {
                try { state.ws.close(); } catch (_) {}
            });

            state.ws.addEventListener("message", event => {
                try {
                    if (typeof event.data === "string") {
                        const msg = JSON.parse(event.data);
                        if (msg.type === "welcome" && msg.player_id) {
                            setConnection(true, msg.player_id);
                            state.packetSeq = 0;
                            state.connectionEpoch++;
                            // Do not clear the local input state here. If the user
                            // touches a button at the exact moment the WebSocket
                            // welcome arrives, clearing it would make that first
                            // press disappear and force a second tap.
                            state.stateDirty = true;
                            sendBinaryState(true);
                        } else if (msg.type === "pong") {
                            const pongNow = performance.now();
                            state.serverLatencyMs = Math.max(0, pongNow - state.lastServerPongSent);
                            state.lastServerPong = pongNow;
                        } else if (msg.type === "server_full") {
                            console.warn("Controller server is full:", msg.max_players);
                        } else if (msg.type === "rumble") {
                            const large = Math.max(0, Math.min(255, Number(msg.large) || 0));
                            const small = Math.max(0, Math.min(255, Number(msg.small) || 0));
                            if (large === 0 && small === 0) {
                                stopGameRumble();
                            } else {
                                startGameRumble(small / 255.0, large / 255.0);
                            }
                        }
                    }
                } catch (e) {}
            });
        } catch (e) {
            setConnection(false);
            setTimeout(connect, 1500);
        }
    }

    function setButtonBit(button, pressed) {
        const mask = BTN_MASKS[button];
        if (mask !== undefined) {
            if (pressed) state.buttonStateMask |= mask;
            else state.buttonStateMask &= ~mask;
        }
        if (button === "LT") state.leftTrigger = pressed ? 255 : 0;
        if (button === "RT") state.rightTrigger = pressed ? 255 : 0;
    }

    function triggerSituationalHaptics(button, pressed) {
        if (!pressed) return;

        switch (button) {
            case "A": Haptics.fifaPass(); break;
            case "Y": Haptics.fifaThroughBall(); break;
            case "B": Haptics.fifaShot(); break;
            case "X": Haptics.fifaTackle(); break;
            case "LT": Haptics.forzaBrakeABS(); break;
            case "RT": Haptics.forzaEngineRumble(255); break;
            default: Haptics.playDualRumble(0.3, 0.3, 30); break;
        }
    }

    const TRIPLE_TAP_WINDOW_MS = 500;
    const TURBO_HOLD_MS = 1000;
    const TURBO_INTERVAL_MS = 45;

    function bindButton(el) {
        const button = el.dataset.btn;
        if (!button || el.dataset.bound) return;

        if (button.startsWith("DPAD_")) return;

        el.dataset.bound = "1";

        let tapCount = 0;
        let lastTapTime = 0;
        let holdTimer = null;
        let turboActive = false;
        let activePointerId = null;

        const stopTurbo = () => {
            if (holdTimer) {
                clearTimeout(holdTimer);
                holdTimer = null;
            }

            const interval = state.turboIntervals.get(button);
            if (interval) {
                clearInterval(interval);
                state.turboIntervals.delete(button);
            }

            turboActive = false;
            el.classList.remove("turbo-active");
        };

        const startTurbo = () => {
            if (turboActive) return;

            turboActive = true;
            el.classList.add("turbo-active");
            Haptics.playDualRumble(0.8, 0.8, 70);

            let turboState = true;
            setButtonBit(button, true);
            sendBinaryState(true);

            const intervalId = setInterval(() => {
                if (!turboActive) return;

                turboState = !turboState;
                setButtonBit(button, turboState);
                sendBinaryState();

                if (turboState) Haptics.vibrate(10);
            }, TURBO_INTERVAL_MS);

            state.turboIntervals.set(button, intervalId);
        };

        const press = e => {
            // Ignore duplicate/non-primary pointer streams. A single physical
            // touch must produce exactly one press event.
            if (activePointerId !== null) return;
            if (e.pointerType === "mouse" && e.button !== 0) return;

            e.preventDefault();
            e.stopPropagation();
            activePointerId = e.pointerId;
            el.setPointerCapture?.(e.pointerId);

            const now = Date.now();

            if (now - lastTapTime > TRIPLE_TAP_WINDOW_MS) {
                tapCount = 0;
            }

            tapCount++;
            lastTapTime = now;

            el.classList.add("pressed");
            triggerSituationalHaptics(button, true);
            setButtonBit(button, true);
            sendBinaryState(true);

            if (tapCount === 3) {
                if (holdTimer) clearTimeout(holdTimer);

                holdTimer = setTimeout(() => {
                    holdTimer = null;
                    startTurbo();
                }, TURBO_HOLD_MS);
            } else if (tapCount > 3) {
                tapCount = 1;
            }
        };

        const release = e => {
            if (activePointerId !== e.pointerId) return;
            e.preventDefault();
            e.stopPropagation();
            activePointerId = null;
            el.classList.remove("pressed");

            if (holdTimer) {
                clearTimeout(holdTimer);
                holdTimer = null;
            }

            stopTurbo();
            setButtonBit(button, false);
            sendBinaryState(true);

            const sequenceAtRelease = tapCount;
            setTimeout(() => {
                if (tapCount === sequenceAtRelease &&
                    Date.now() - lastTapTime > TRIPLE_TAP_WINDOW_MS) {
                    tapCount = 0;
                }
            }, TRIPLE_TAP_WINDOW_MS + 25);
        };

        el.addEventListener("pointerdown", press);
        el.addEventListener("pointerup", release);
        el.addEventListener("pointercancel", release);
        el.addEventListener("lostpointercapture", release);
        el.addEventListener("pointerleave", e => {
            if (e.buttons && activePointerId === e.pointerId) release(e);
        });
    }

    function bindDpad(el) {
        if (!el || el.dataset.dpadBound) return;
        el.dataset.dpadBound = "1";

        let activePointerId = null;
        let activeDirections = new Set();

        const directionFromPoint = (clientX, clientY) => {
            const r = el.getBoundingClientRect();
            const x = clientX - (r.left + r.width / 2);
            const y = clientY - (r.top + r.height / 2);

            const radiusX = r.width * 0.52;
            const radiusY = r.height * 0.52;
            if (Math.abs(x) > radiusX || Math.abs(y) > radiusY) {
                return new Set();
            }

            const normalizedX = x / (r.width * 0.5);
            const normalizedY = y / (r.height * 0.5);
            if (Math.hypot(normalizedX, normalizedY) < 0.22) {
                return new Set();
            }

            const angle = Math.atan2(y, x) * 180 / Math.PI;

            const directions = new Set();

            if (angle >= -22.5 && angle < 22.5) {
                directions.add("DPAD_RIGHT");
            } else if (angle >= 22.5 && angle < 67.5) {
                directions.add("DPAD_RIGHT");
                directions.add("DPAD_DOWN");
            } else if (angle >= 67.5 && angle < 112.5) {
                directions.add("DPAD_DOWN");
            } else if (angle >= 112.5 && angle < 157.5) {
                directions.add("DPAD_LEFT");
                directions.add("DPAD_DOWN");
            } else if (angle >= 157.5 || angle < -157.5) {
                directions.add("DPAD_LEFT");
            } else if (angle >= -157.5 && angle < -112.5) {
                directions.add("DPAD_LEFT");
                directions.add("DPAD_UP");
            } else if (angle >= -112.5 && angle < -67.5) {
                directions.add("DPAD_UP");
            } else {
                directions.add("DPAD_RIGHT");
                directions.add("DPAD_UP");
            }

            return directions;
        };

        const applyDirections = directions => {
            const changed =
                directions.size !== activeDirections.size ||
                [...directions].some(d => !activeDirections.has(d));

            if (!changed) return;

            for (const direction of activeDirections) {
                if (!directions.has(direction)) {
                    setButtonBit(direction, false);
                    el.querySelector(`[data-btn="${direction}"]`)?.classList.remove("pressed");
                }
            }

            for (const direction of directions) {
                if (!activeDirections.has(direction)) {
                    setButtonBit(direction, true);
                    el.querySelector(`[data-btn="${direction}"]`)?.classList.add("pressed");
                    triggerSituationalHaptics(direction, true);
                }
            }

            activeDirections = new Set(directions);
            sendBinaryState(true);
        };

        const updateFromPoint = e => {
            if (activePointerId !== e.pointerId) return;
            e.preventDefault();
            applyDirections(directionFromPoint(e.clientX, e.clientY));
        };

        const end = e => {
            if (activePointerId !== e.pointerId) return;
            e.preventDefault();

            for (const direction of activeDirections) {
                setButtonBit(direction, false);
                el.querySelector(`[data-btn="${direction}"]`)?.classList.remove("pressed");
            }

            activeDirections.clear();
            activePointerId = null;
            sendBinaryState(true);
        };

        el.addEventListener("pointerdown", e => {
            if (activePointerId !== null) return;

            e.preventDefault();
            e.stopPropagation();

            activePointerId = e.pointerId;
            el.setPointerCapture?.(e.pointerId);
            applyDirections(directionFromPoint(e.clientX, e.clientY));
        });

        el.addEventListener("pointermove", updateFromPoint);
        el.addEventListener("pointerup", end);
        el.addEventListener("pointercancel", end);
        el.addEventListener("lostpointercapture", end);
    }

    function bindJoystick(el) {
        if (el.dataset.bound) return;
        el.dataset.bound = "1";

        const stick = el.dataset.joystick || "left";
        let animationFrame = null;

        const processMove = (x, y) => {
            updateKnobVisual(el, x, y);

            if (stick === "right") {
                if (state.gyroModeIndex !== 2) {
                    state.stickState.rx = x;
                    state.stickState.ry = y;
                }
            } else {
                if (state.gyroModeIndex !== 1) {
                    state.stickState.lx = x;
                    state.stickState.ly = y;
                }
            }

            sendBinaryState();
        };

        const move = e => {
            if (!state.joystickActive.has(e.pointerId)) return;
            e.preventDefault();

            const r = el.getBoundingClientRect();
            const cx = r.left + r.width / 2;
            const cy = r.top + r.height / 2;
            const dx = e.clientX - cx;
            const dy = e.clientY - cy;
            const distance = Math.hypot(dx, dy) || 1;
            const radius = r.width * 0.38;
            const scale = Math.min(1, radius / distance);
            const x = (dx * scale) / radius;
            const y = (dy * scale) / radius;

            if (animationFrame) cancelAnimationFrame(animationFrame);
            animationFrame = requestAnimationFrame(() => processMove(x, y));
        };

        const end = e => {
            if (!state.joystickActive.has(e.pointerId)) return;
            state.joystickActive.delete(e.pointerId);
            if (animationFrame) cancelAnimationFrame(animationFrame);
            if ((stick === "right" && state.gyroModeIndex !== 2) ||
                (stick === "left" && state.gyroModeIndex !== 1)) {
                updateKnobVisual(el, 0, 0);
            }

            if (stick === "right") {
                if (state.gyroModeIndex !== 2) {
                    state.stickState.rx = 0;
                    state.stickState.ry = 0;
                }
            } else {
                if (state.gyroModeIndex !== 1) {
                    state.stickState.lx = 0;
                    state.stickState.ly = 0;
                }
            }
            sendBinaryState();
        };

        el.addEventListener("pointerdown", e => {
            e.preventDefault();
            el.setPointerCapture?.(e.pointerId);
            state.joystickActive.set(e.pointerId, true);
            move(e);
        });
        el.addEventListener("pointermove", move);
        el.addEventListener("pointerup", end);
        el.addEventListener("pointercancel", end);
    }

    $$("[data-btn]").forEach(bindButton);
    $$(".dpad").forEach(bindDpad);
    $$("[data-joystick]").forEach(bindJoystick);

    const keyMap = {
        w: "DPAD_UP", s: "DPAD_DOWN", a: "DPAD_LEFT", d: "DPAD_RIGHT",
        " ": "A", e: "B", q: "X", r: "Y",
        Shift: "LB", Control: "RB", ArrowUp: "DPAD_UP", ArrowDown: "DPAD_DOWN",
        ArrowLeft: "DPAD_LEFT", ArrowRight: "DPAD_RIGHT"
    };

    const activeKeys = new Set();

    window.addEventListener("keydown", e => {
        if (e.repeat || !keyMap[e.key]) return;
        activeKeys.add(e.key);
        const btn = keyMap[e.key];
        const btnEl = $(`[data-btn="${btn}"]`);
        if (btnEl) btnEl.classList.add("pressed");
        triggerSituationalHaptics(btn, true);
        setButtonBit(btn, true);
        sendBinaryState(true);
    });

    window.addEventListener("keyup", e => {
        if (!activeKeys.has(e.key)) return;
        activeKeys.delete(e.key);
        const btn = keyMap[e.key];
        const btnEl = $(`[data-btn="${btn}"]`);
        if (btnEl) btnEl.classList.remove("pressed");
        setButtonBit(btn, false);
        sendBinaryState(true);
    });

    window.addEventListener("contextmenu", e => e.preventDefault());

    window.controllerDiagnostics = () => ({
        playerId: state.playerId,
        connected: state.connected,
        gyro: { enabled: state.gyroEnabled, ready: state.gyroReady, source: state.gyroSensorSource },
        packetSeq: state.packetSeq,
        lastInputMs: Math.round(performance.now() - state.lastInputAt),
        reconnectDelayMs: Math.round(state.reconnectDelay)
    });

    handleOrientationChange();
    connect();
})();
</script>
</body>
</html>
"##;