export type DateTimeString = string;
export type NodeID = string;
export type TaskID = string;
export type ThemeMode = "dark" | "light";
export type TaskStatus =
  | "pending"
  | "running"
  | "synthesizing"
  | "awaiting_approval"
  | "completed"
  | "failed"
  | "cancelled";
export type DispatchAttemptStatus = "running" | "succeeded" | "failed";
export type DeviceKind = "cpu" | "gpu" | "npu";
export type TelemetryState = "fresh" | "stale" | "unavailable";
export type DiscoveryAuthentication = "unauthenticated" | "hmac-sha256" | "unavailable";
export type NodeServiceAction = "start" | "stop" | "restart" | "drain" | "upgrade";
export type DashboardTabID = "cluster-tab" | "memory-tab" | "agent-tab";

export interface HardwareDevice {
  kind: DeviceKind;
  vendor?: string;
  name?: string;
  index?: number;
  memory_mb?: number;
  free_memory_mb?: number;
  unified_memory?: boolean;
  driver?: string;
  backends?: string[];
  [property: string]: unknown;
}

export interface NetworkLink {
  name: string;
  speed_mbps?: number;
  rdma?: boolean;
  trusted?: boolean;
  [property: string]: unknown;
}

export interface NodeHardware {
  os: string;
  arch: string;
  cpu_cores: number;
  cpu_stats_available: boolean;
  cpu_utilization_percent?: number;
  total_ram_mb: number;
  memory_stats_available: boolean;
  memory_usage_percent?: number;
  has_gpu: boolean;
  gpu_vendor?: string;
  gpu_stats_available: boolean;
  gpu_utilization_percent?: number;
  gpu_memory_free_mb?: number;
  has_npu: boolean;
  npu_vendor?: string;
  npu_stats_available: boolean;
  npu_utilization_percent?: number;
  devices?: HardwareDevice[] | null;
  numa_nodes?: number[];
  network_links?: NetworkLink[] | null;
  score: number;
  [property: string]: unknown;
}

export interface NodeModel {
  name: string;
  engine: string;
  tags: string[] | null;
  protocols?: string[];
  size_gb?: number;
  context_length?: number;
  device?: DeviceKind;
  quantization?: string;
  default?: boolean;
  enabled?: boolean;
  [property: string]: unknown;
}

export interface EngineStatus {
  name: string;
  ready: boolean;
  protocols?: string[];
  capabilities?: Record<string, unknown>;
  capabilities_error?: string;
  inventory_state?: "unknown" | "available" | "empty" | "error";
  model_count: number;
  last_checked_at?: DateTimeString;
  last_successful_at?: DateTimeString;
  last_error?: string;
  [property: string]: unknown;
}

export interface GPUUsageSample {
  timestamp: DateTimeString;
  available: boolean;
  utilization_percent: number;
  [property: string]: unknown;
}

export interface NPUUsageSample extends GPUUsageSample {}

export interface SystemUsageSample {
  timestamp: DateTimeString;
  cpu_available: boolean;
  cpu_utilization_percent: number;
  memory_available: boolean;
  memory_usage_percent: number;
  [property: string]: unknown;
}

export interface UpgradeStatus {
  state: "queued" | "starting" | "running" | "restarting" | "rejoining" | "completed" | "failed" | "unknown";
  phase: string;
  progress: number;
  ticket_status: "not_requested" | "requesting" | "ready" | "consumed" | "expired" | "rejected" | "failed";
  failure_reason?: string;
  updated_at: DateTimeString;
}

export interface NodeStatus {
  id: NodeID;
  version: string;
  address: string;
  control_address?: string;
  draining?: boolean;
  upgrade_status?: UpgradeStatus;
  hardware: NodeHardware;
  models: NodeModel[] | null;
  engines?: EngineStatus[];
  supported_shard_models?: string[];
  max_layers_it_can_host?: number;
  allow_insecure_shard_network?: boolean;
  current_stage_assignments?: number;
  capabilities?: string[];
  gpu_history?: GPUUsageSample[];
  npu_history?: NPUUsageSample[];
  system_history?: SystemUsageSample[];
  telemetry_at?: DateTimeString;
  default_model?: string;
  fast_score: number;
  active_tasks: number;
  max_concurrent_tasks?: number;
  available_capacity?: number;
  last_seen: DateTimeString;
  healthy: boolean;
  leader: boolean;
  version_error?: string;
  [property: string]: unknown;
}

export interface Subtask {
  id: string;
  description: string;
  role?: string;
  task_type: string;
  tags: string[] | null;
  depends_on?: string[];
  expected_output?: string;
  output_format?: string;
  required_fields?: string[];
  max_retries?: number;
  required_capabilities?: string[];
  preferred_capabilities?: string[];
  min_model_size_gb?: number;
  preferred_model?: string;
  routing_preference?: "quality" | "latency";
  synthesis?: boolean;
  [property: string]: unknown;
}

export interface ScoreFactors {
  model_match: number;
  hardware: number;
  active_tasks: number;
  queued_tasks: number;
  pending_dependents?: number;
  capacity_penalty?: number;
  load_penalty: number;
  gpu_compatibility_score?: number;
  gpu_utilization_penalty: number;
  npu_utilization_penalty: number;
  cpu_utilization_penalty?: number;
  memory_usage_penalty?: number;
  capability_score?: number;
  matched_capabilities?: string[];
  model_quality_score?: number;
  topology_score?: number;
  estimated_completion_seconds?: number;
  estimated_tokens_per_second?: number;
  model_size_gb?: number;
  latency_score?: number;
  learned_bias: number;
  telemetry_fresh: boolean;
  telemetry_age_ms?: number;
  [property: string]: unknown;
}

export interface DispatchAttempt {
  id: string;
  request_id?: string;
  number: number;
  node_id: NodeID;
  model: string;
  engine?: string;
  status: DispatchAttemptStatus;
  failure_class?: string;
  http_status?: number;
  error?: string;
  started_at: DateTimeString;
  finished_at?: DateTimeString;
  duration_ms: number;
  [property: string]: unknown;
}

export interface SubtaskResult {
  subtask: Subtask;
  node_id: NodeID;
  address: string;
  model: string;
  engine?: string;
  score: number;
  score_factors: ScoreFactors;
  status: string;
  output?: string;
  error?: string;
  attempts?: DispatchAttempt[];
  [property: string]: unknown;
}

export interface Task {
  id: TaskID;
  origin?: string;
  sequence?: number;
  description: string;
  status: TaskStatus;
  subtasks: SubtaskResult[] | null;
  graph?: Record<string, unknown>;
  resumable?: boolean;
  last_checkpoint?: string;
  require_approval?: boolean;
  result?: string;
  error?: string;
  created_at: DateTimeString;
  updated_at: DateTimeString;
  [property: string]: unknown;
}

export interface TaskHistoryPageResponse {
  items?: Task[] | null;
  next_cursor?: string;
  has_more?: boolean;
}

export type TaskHistoryResponse = Task[] | TaskHistoryPageResponse;

export interface NormalizedTaskHistoryPage {
  items: Task[];
  next_cursor: string;
  has_more: boolean;
}

export interface DiscoveryPeer {
  role: "orchestrator" | "node";
  id: string;
  http_addr: string;
  peer_addr: string;
  score?: number;
  members?: number;
  elected?: boolean;
  orchestrator_id?: string;
  issued_at_unix_nano?: number;
  hmac_sha256?: string;
  last_seen: DateTimeString;
  [property: string]: unknown;
}

export interface NodeCapacityHealth {
  node_id: NodeID;
  healthy: boolean;
  active: number;
  max: number;
  remaining: number;
  oversubscribed: boolean;
  telemetry_state: TelemetryState;
  telemetry_age_ms?: number;
}

export interface ClusterHealth {
  nodes: NodeCapacityHealth[];
  global_dispatch: {
    global_limit: number;
    active_slots: number;
    queue_depth: number;
    active_workers: number;
  };
  engine_readiness: {
    ready_engines: number;
    total_engines: number;
    nodes_with_engines: number;
    nodes_with_ready_engine: number;
  };
  telemetry_freshness: {
    fresh_nodes: number;
    stale_nodes: number;
    unavailable_nodes: number;
    freshness_window_ms: number;
  };
  warnings: string[];
}

export interface DashboardModelInventory {
  model_id: string;
  kind: "whole" | "sharded";
  nodes?: NodeID[];
}

export interface DashboardPipelineStage {
  stage: number;
  node_id: NodeID;
  start_layer: number;
  end_layer: number;
  engine: string;
  status: "ready" | "offline" | "unavailable";
  active: number;
  capacity?: number;
  utilization_percent?: number;
  activation_transfer_ms?: number;
  activation_bytes?: number;
}

export interface DashboardPipelinePlan {
  model_id: string;
  status: "not_placed" | "ready" | "degraded";
  plan?: Record<string, unknown>;
  stages: DashboardPipelineStage[];
}

export interface ShardGroup {
  id: string;
  model?: Record<string, unknown>;
  mode: "llamacpp-rpc" | "vllm-mp";
  state:
    | "planning"
    | "starting_workers"
    | "starting_head"
    | "ready"
    | "degraded"
    | "failed"
    | "stopping"
    | "stopped";
  plan?: Record<string, unknown>;
  members: Array<{
    node_id: NodeID;
    address: string;
    role: "worker" | "head";
    engine_id: string;
    engine_kind: "llama-server" | "ggml-rpc-server" | "vllm";
    rank?: number;
    started?: boolean;
  }>;
  pinned_nodes?: NodeID[];
  rationale: string[];
  error?: string;
  created_at: DateTimeString;
  updated_at: DateTimeString;
}

export interface DashboardDispatchSnapshot {
  global_limit: number;
  active_global_slots: number;
  active_dispatch_workers: number;
  queue_depth: number;
  dependency_waiters?: number;
  dependency_wait_ms_total?: number;
  dispatch_ms_total?: number;
  goroutines?: number;
}

export interface RouterRequestEvent {
  request_id: string;
  attempt_id?: string;
  phase: string;
  timestamp: DateTimeString;
  node_id?: NodeID;
  model?: string;
  engine?: string;
  queue_ms?: number;
  time_to_first_byte_ms?: number;
  status?: number;
  failure_class?: string;
  [property: string]: unknown;
}

export interface RouterCircuitBreaker {
  node_id: NodeID;
  model: string;
  state: "closed" | "open" | "half_open";
  recent_failures: number;
  open_until?: DateTimeString;
  probe_in_flight?: boolean;
}

export interface MemoryInsights {
  total_experiences?: number;
  success_rate?: number;
  [property: string]: unknown;
}

export interface AgentStats {
  [property: string]: unknown;
}

export interface DashboardSnapshot {
  nodes?: NodeStatus[] | null;
  tasks?: Task[] | null;
  complete_tasks?: boolean;
  discovery?: DiscoveryPeer[] | null;
  discovery_authentication?: DiscoveryAuthentication;
  build_version?: string;
  leader_id?: string;
  member_count?: number;
  agent_stats?: AgentStats | null;
  memory?: MemoryInsights | null;
  dispatch?: DashboardDispatchSnapshot | null;
  cluster_health?: ClusterHealth | null;
  router_events?: RouterRequestEvent[] | null;
  router_circuit_breakers?: RouterCircuitBreaker[] | null;
  model_inventory?: DashboardModelInventory[] | null;
  pipeline_plans?: DashboardPipelinePlan[] | null;
  [property: string]: unknown;
}

export interface PairingStatus {
  role?: "orchestrator" | "node";
  node_id?: NodeID;
  orchestrator_id?: string;
  orchestrator_addr?: string;
  address?: string;
  dashboard_url?: string;
  fingerprint?: string;
  pin?: string;
  expires_at?: DateTimeString;
  joined?: boolean;
  require_pairing_verification?: boolean;
  [property: string]: unknown;
}

export interface PairingTargetNode {
  id: NodeID;
  address?: string;
  peer_addr?: string;
  joinOrchestrator?: boolean;
  discovered?: boolean;
  [property: string]: unknown;
}

export interface PairingUIState {
  status: PairingStatus | null;
  targetNode: PairingTargetNode | null;
  setupFocused: boolean;
  promptShown: boolean;
  dialogAutoOpened: boolean;
}

export interface ConnectionState {
  connected: boolean;
  lastSyncAt: number | null;
}

export interface AgentSessionSummary {
  id?: string;
  harness?: string;
  title?: string;
  updated_at?: DateTimeString;
  [property: string]: unknown;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

export interface MemoryHistoryPageState<T> {
  items: T[];
  nextCursor: string;
  hasMore: boolean;
  loading: boolean;
  generation: number;
  url?: string;
}

export interface TaskHistoryPageState<T> extends MemoryHistoryPageState<T> {
  initialized: boolean;
  filterKey: string;
}

export interface DashboardState {
  nodes: NodeStatus[];
  tasks: Task[];
  sessions: AgentSessionSummary[];
  pairing: PairingUIState;
  connection: ConnectionState;
  capacity: ClusterHealth | null;
  modelInventory: DashboardModelInventory[];
  pipelinePlans: DashboardPipelinePlan[];
  shardGroups: ShardGroup[];
  shardGroupsError: string;
  discovery: DiscoveryPeer[];
  discoveryAuthentication: DiscoveryAuthentication;
  buildVersion: string;
  fullTaskHistoryLoaded: boolean;
  renderSignatures: Record<string, string>;
}

export type ActionHandler<Args extends unknown[] = []> =
  (...args: Args) => void | Promise<void>;
export type DOMActionHandler<E extends Event> = ActionHandler<[event: E]>;

export interface DashboardActionHandlers {
  refresh: ActionHandler;
  refreshTaskHistory: ActionHandler<[reset?: boolean]>;
  loadMoreTaskHistory: ActionHandler;
  applySnapshot: ActionHandler<[snapshot: DashboardSnapshot]>;
  connectionChange: ActionHandler<[connected: boolean]>;
  activateDashboardTab: ActionHandler<[tabID: DashboardTabID]>;
  activateWorkspacePanel: ActionHandler<[panelID: string]>;
  submitTask: DOMActionHandler<MouseEvent>;
}

export interface TaskActionHandlers {
  cancel: ActionHandler<[taskID: TaskID]>;
  approve: ActionHandler<[taskID: TaskID]>;
  resume: ActionHandler<[taskID: TaskID]>;
}

export interface NodeActionHandlers {
  openTerminal: ActionHandler<[node: NodeStatus]>;
  refreshModels: ActionHandler<[nodeID: NodeID]>;
  controlService: ActionHandler<
    [nodeID: NodeID, action: NodeServiceAction, button: HTMLButtonElement]
  >;
}

export interface PairingActionHandlers {
  open: ActionHandler<[node?: PairingTargetNode | null]>;
  connect: DOMActionHandler<SubmitEvent>;
  acceptInvitation: DOMActionHandler<MouseEvent>;
  declineInvitation: DOMActionHandler<MouseEvent>;
}

export interface ThemeActionHandlers {
  setTheme: ActionHandler<[mode: ThemeMode, persist?: boolean]>;
  toggleTheme: DOMActionHandler<MouseEvent>;
}

export interface AgentActionHandlers {
  sendMessage: ActionHandler<[message: string]>;
  interrupt: DOMActionHandler<MouseEvent>;
  clearMessages: ActionHandler;
}
