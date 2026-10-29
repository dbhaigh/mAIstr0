# Node service controller

The controller is a mode of the `maistr0` executable, not a second executable.
The OS service runs `maistr0 controller`; that controller process supervises a
child `maistr0` process that runs the cluster application. This keeps Start,
Stop, Restart, and Drain available even when the application child is stopped.

The controller advertises an HTTPS endpoint on TCP port `7453` by default.
Allow inbound cluster-LAN traffic to that port, plus the normal node API and
discovery ports. Its API requires the paired current orchestrator's mutual-TLS
identity; do not expose the port to the public Internet. The controller and
application child must run as the same OS account so they share the node
identity, trust roster, and runtime settings.

## Linux with systemd

Install the single `maistr0` executable in `$HOME/.local/bin`, then install
the packaged user unit and start it:

```sh
mkdir -p "$HOME/.local/bin" "$HOME/.config/systemd/user"
install -m 0755 maistr0 "$HOME/.local/bin/"
install -m 0644 packaging/systemd/maistr0.service "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user enable --now maistr0.service
```

The unit runs `maistr0 controller --service-managed`. Persistent dashboard
settings are read from the same user's `~/.config/maistr0/runtime.json`. To use
different config files, save those paths in the dashboard settings before
restarting the service.

For an existing installation, stop the old service, install the new single
`maistr0` executable and unit, then run `systemctl --user daemon-reload` and
start the service again. The old `maistr0-controller` file is no longer used.

## macOS LaunchAgent

The macOS CLI archive contains a per-user LaunchAgent installer. Extract the
archive, then run:

```sh
bash packaging/launchd/install.sh
```

The installer copies the executable and Darwin update bundles into
`~/.local`, writes `~/Library/LaunchAgents/com.maistr0.agent.plist` with
the absolute executable path, and starts `maistr0 controller
--service-managed` in the current user's GUI session. This account shares the
same identity and runtime settings as the dashboard. The service runs at login
and restarts after unexpected exits.

To stop the service without removing it, run:

```sh
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.maistr0.agent.plist"
```

To start it again, run:

```sh
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.maistr0.agent.plist"
```

To uninstall the LaunchAgent and executable while retaining configuration and
data, run `bash packaging/launchd/uninstall.sh` from the extracted archive.
The menu-bar `.app` ZIP is separate; move it into a user-writable `~/Applications`
folder if you want peer updates to replace its executable. Packages are
unsigned and not notarized, so macOS Gatekeeper may require explicit approval
the first time the app is opened. Control-click the app in Finder, choose
**Open**, and confirm the warning to launch it.

## Windows Service

Install the single `maistr0.exe` in a directory writable only by the account
that will run mAIstr0. Create the service from an elevated PowerShell prompt,
substituting the actual installation directory:

```powershell
$bin = "$env:LOCALAPPDATA\mAIstr0\bin"
$application = Join-Path $bin "maistr0.exe"
$imagePath = "`"$application`" controller"
sc.exe create mAIstr0HostController binPath= $imagePath start= auto
sc.exe failure mAIstr0HostController reset= 86400 actions= restart/30000/restart/30000/restart/30000
sc.exe failureflag mAIstr0HostController 1
```

For an existing service, stop it, install the new `maistr0.exe`, update its
image path with `sc.exe config mAIstr0HostController binPath= $imagePath`, and
configure the recovery action shown above. The default Local System account has
a different identity and runtime-settings directory, so configure **Log On**
in `services.msc` to use the same Windows account that paired the node.
Restrict the installation and configuration directories to that account and
administrators.

Start and inspect the service with:

```powershell
Start-Service mAIstr0HostController
Get-Service mAIstr0HostController
```

The recovery action is required for automatic peer updates: on Windows, the
controller and application share an executable image, so the controller exits
to release the file before replacement. The update helper replaces the
executable, then the service manager starts the new controller mode.

## Dashboard lifecycle behavior

- **Start** launches the application process.
- **Stop** requests graceful shutdown, then terminates the child only if it
  fails to exit within the controller's timeout.
- **Drain & stop** rejects newly submitted work, waits for current work to
  finish, and then stops the process. A drain that exceeds 30 minutes stays
  draining and reports its error rather than silently accepting new work.
- **Restart** drains current work before stopping and starting the application.

The dashboard shows these controls for nodes that advertise a controller
address. If the application is stopped, the controller uses authenticated LAN
discovery to verify the current elected orchestrator before accepting remote
start or lifecycle requests. Consequently, a stopped node configured with
`--no-discovery` cannot be started remotely; start it through the local OS
service manager or enable discovery in its persistent settings.

`--allow-peer-updates` remains opt-in: it grants paired publishers authority to
run executable code as the node's account.
