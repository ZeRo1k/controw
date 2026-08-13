# 📱 Web Controller Server

Turn your phone into a controller. No app, no cables, no hassle.

Run the server on your PC, scan the QR code from the terminal with your phone, and you're ready to go over your local Wi-Fi.

## ✨ Features

* **⚡ Quick Pairing** — Scan the QR code and connect instantly.
* **🌐 Web Controller** — Works directly in your mobile browser. Nothing to install.
* **🎮 Multiple Players** — Supports multiple connected players with automatic player slots.
* **📊 Live TUI** — See connected players, latency, packet rates, and server events right in the terminal.
* **⚡ Low Latency** — Built around WebSockets for fast, responsive input.
* **📱 Mobile Friendly** — Designed to work across different phone screen sizes and orientations.
* **🎮 Virtual Xbox Controller** — Uses ViGEm to expose connected players as virtual Xbox controllers on Windows.

## 🖥️ Requirements

* Windows PC
* Local Wi-Fi network
* Modern mobile browser
* **ViGEmBus** installed

> ViGEmBus is required for virtual Xbox controller support.

## 🚀 Getting Started

1. Install **ViGEmBus** on your PC.
2. Start the Web Controller Server.
3. The terminal TUI will display the server status and QR code.
4. Scan the QR code with your phone.
5. Open the controller in your browser.
6. Start playing.

Your phone and PC need to be connected to the same local network.

## 🎮 Player System

Each connected device gets its own player slot.

For example:

```text
Player 1   ● Connected
Player 2   ● Connected
Player 3   ○ Waiting
Player 4   ○ Waiting
```

When a player disconnects, their slot becomes available again instead of permanently increasing the player number.

## 🖥️ Terminal UI

The server includes a live terminal interface instead of dumping connection messages into the console.

It shows:

* Connected players
* Player status
* Connection latency
* Packet activity
* Server events
* QR code
* Available controller layouts

Everything you need is visible from one screen.

## 🔧 Controller Layouts

Hold the **Layout** button on the controller to open the layout menu and switch between available control schemes.

The controller is designed to adapt to different phones and screen orientations.

## ⚙️ Architecture

```text
┌──────────────┐
│    Phone     │
│ Web Browser  │
└──────┬───────┘
       │ WebSocket
       ▼
┌──────────────┐
│ Web Controller│
│    Server    │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    ViGEm     │
│ Virtual Xbox │
│  Controller  │
└──────┬───────┘
       │
       ▼
    Windows
```

## 📌 Notes

This project is intended for use on a trusted local network.

**ViGEmBus is required** for virtual Xbox controller functionality on Windows.

---

Made for quick local multiplayer, couch gaming, and turning whatever phone is nearby into a controller.
