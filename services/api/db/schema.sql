-- === 基础：审计与版本 ===
CREATE TABLE IF NOT EXISTS audit_log (
  id BIGSERIAL PRIMARY KEY,
  run_id VARCHAR(64),
  actor VARCHAR(32) DEFAULT 'system',
  action VARCHAR(64),
  entity_type VARCHAR(32),
  entity_id VARCHAR(64),
  payload JSONB,
  snapshot_ts TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- === DPC：Schema 注册与质量规则（口径与约束的元数据） ===
CREATE TABLE IF NOT EXISTS dpc_schema_registry (
  id BIGSERIAL PRIMARY KEY,
  schema_name VARCHAR(64) NOT NULL,
  schema_version VARCHAR(16) NOT NULL,
  definition JSONB NOT NULL,             -- 字段定义/类型/必填/取值域
  status VARCHAR(16) DEFAULT 'active',   -- active/deprecated
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(schema_name, schema_version)
);

CREATE TABLE IF NOT EXISTS dpc_quality_rule (
  id BIGSERIAL PRIMARY KEY,
  rule_name VARCHAR(64) NOT NULL,
  entity VARCHAR(32) NOT NULL,           -- 作用对象：player/team/match/referee/ingest_raw 等
  rule_type VARCHAR(32) NOT NULL,        -- null_ratio/bounds/jump/psi/duplicate 等
  params JSONB NOT NULL,                 -- 阈值、分位等
  severity VARCHAR(16) DEFAULT 'warn',   -- warn/block
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(rule_name, entity)
);

-- === 主数据：球员 UID 与别名/XREF ===
CREATE TABLE IF NOT EXISTS dim_player (
  player_uid VARCHAR(40) PRIMARY KEY,    -- 例如 plr_global_xxx
  name VARCHAR(128) NOT NULL,            -- 展示名（可更新）
  birth_date DATE,
  birth_time TIME,                       -- 可能缺失；反推后回填
  birth_place VARCHAR(128),
  team_uid VARCHAR(40),
  position VARCHAR(32),
  jersey_no VARCHAR(8),
  importance_level CHAR(1),              -- A/B/C/D
  importance_score NUMERIC(4,3),         -- 0~1
  confidence NUMERIC(4,3) DEFAULT 1.0,   -- 记录当前档案置信度
  merged_into VARCHAR(40),               -- 若被合并，指向主 UID
  lifecycle_status VARCHAR(16) DEFAULT 'active', -- active/retired/merged
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_player_team ON dim_player(team_uid);
CREATE INDEX IF NOT EXISTS idx_dim_player_name ON dim_player(name);

CREATE TABLE IF NOT EXISTS player_alias (
  id BIGSERIAL PRIMARY KEY,
  player_uid VARCHAR(40) NOT NULL REFERENCES dim_player(player_uid) ON DELETE CASCADE,
  name VARCHAR(128) NOT NULL,
  lang VARCHAR(16) DEFAULT 'und',
  source VARCHAR(64),
  confidence NUMERIC(4,3) DEFAULT 0.9,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_player_alias_name ON player_alias(name);

CREATE TABLE IF NOT EXISTS player_xref (
  id BIGSERIAL PRIMARY KEY,
  player_uid VARCHAR(40) NOT NULL REFERENCES dim_player(player_uid) ON DELETE CASCADE,
  provider VARCHAR(64) NOT NULL,         -- apifootball/transfermarkt/sofa…
  provider_player_id VARCHAR(128) NOT NULL,
  first_seen_at TIMESTAMPTZ DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(provider, provider_player_id)
);

CREATE TABLE IF NOT EXISTS player_merge_event (
  id BIGSERIAL PRIMARY KEY,
  from_uid VARCHAR(40) NOT NULL,
  to_uid VARCHAR(40) NOT NULL,
  reason VARCHAR(128),
  decided_by VARCHAR(64) DEFAULT 'system',
  decided_at TIMESTAMPTZ DEFAULT NOW()
);

-- === 赛程事实表（最小） ===
CREATE TABLE IF NOT EXISTS f_match (
  match_id VARCHAR(64) PRIMARY KEY,
  league_id VARCHAR(32),
  season VARCHAR(16),
  round VARCHAR(16),
  home_team_uid VARCHAR(40),
  away_team_uid VARCHAR(40),
  kickoff_time_utc TIMESTAMPTZ,
  kickoff_tz VARCHAR(40) DEFAULT 'UTC',
  venue_city VARCHAR(64),
  ref_uid VARCHAR(40),
  status VARCHAR(16) DEFAULT 'scheduled', -- scheduled/live/finished/postponed
  odds_home NUMERIC(6,3),
  odds_draw NUMERIC(6,3),
  odds_away NUMERIC(6,3),
  result_1x2 CHAR(1),                    -- H/D/A（完赛后回填）
  home_goals SMALLINT,
  away_goals SMALLINT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_f_match_league_season ON f_match(league_id, season);
CREATE INDEX IF NOT EXISTS idx_f_match_kickoff ON f_match(kickoff_time_utc);

-- === DPC 写入审计（统一入口产物的落库轨迹） ===
CREATE TABLE IF NOT EXISTS dpc_ingest_audit (
  id BIGSERIAL PRIMARY KEY,
  run_id VARCHAR(64),
  source_id VARCHAR(128),                 -- 抓取来源标识或供应商批次
  entity_type VARCHAR(32),                -- player/team/match/referee/news…
  entity_id VARCHAR(128),
  action VARCHAR(32) DEFAULT 'ingest',    -- ingest/normalize/merge/update
  confidence NUMERIC(4,3),
  signature VARCHAR(128),                 -- 去重指纹
  status VARCHAR(16) DEFAULT 'accepted',  -- accepted/warn/blocked
  message TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
