CREATE TABLE "files" (
  "id" bigserial PRIMARY KEY,
  "filename" varchar UNIQUE NOT NULL,
  "file_hash" varchar(64) NOT NULL,
  "status" varchar DEFAULT 'pending',
  "rows_processed" integer DEFAULT 0,
  "rows_failed" integer DEFAULT 0,
  "error_message" text,
  "created_at" timestamptz DEFAULT (now()),
  "updated_at" timestamptz DEFAULT (now())
);

CREATE TABLE "device_data" (
  "id" bigserial PRIMARY KEY,
  "file_id" bigint NOT NULL,
  "unit_guid" uuid NOT NULL,
  "mqtt" varchar,
  "invid" varchar,
  "msg_id" varchar,
  "text" text,
  "context" varchar,
  "class" varchar,
  "level" integer,
  "area" varchar,
  "addr" varchar,
  "block" varchar,
  "type" varchar,
  "bit" integer,
  "invert_bit" boolean DEFAULT false,
  "line_number" integer NOT NULL,
  "created_at" timestamptz DEFAULT (now())
);

CREATE TABLE "processing_errors" (
  "id" bigserial PRIMARY KEY,
  "file_id" bigint NOT NULL,
  "line_number" integer,
  "raw_line" text,
  "error_message" text NOT NULL,
  "field_name" varchar,
  "created_at" timestamptz DEFAULT (now())
);

CREATE TABLE "reports" (
  "id" bigserial PRIMARY KEY,
  "unit_guid" uuid NOT NULL,
  "report_type" varchar DEFAULT 'pdf',
  "file_path" varchar NOT NULL,
  "generated_at" timestamptz DEFAULT (now())
);

CREATE TABLE "api_logs" (
  "id" bigserial PRIMARY KEY,
  "endpoint" varchar NOT NULL,
  "unit_guid" uuid,
  "response_time_ms" integer,
  "status_code" integer,
  "created_at" timestamptz DEFAULT (now())
);

CREATE INDEX ON "files" ("filename");

CREATE INDEX ON "files" ("status");

CREATE INDEX ON "files" ("created_at");

CREATE INDEX ON "device_data" ("unit_guid");

CREATE INDEX ON "device_data" ("file_id");

CREATE INDEX ON "device_data" ("class");

CREATE INDEX ON "device_data" ("msg_id");

CREATE INDEX "pagination_index" ON "device_data" ("unit_guid", "id");

CREATE INDEX ON "processing_errors" ("file_id");

CREATE INDEX ON "processing_errors" ("created_at");

CREATE INDEX ON "reports" ("unit_guid");

CREATE INDEX ON "reports" ("generated_at");

CREATE INDEX ON "api_logs" ("unit_guid");

CREATE INDEX ON "api_logs" ("created_at");

ALTER TABLE "device_data" ADD FOREIGN KEY ("file_id") REFERENCES "files" ("id");

ALTER TABLE "processing_errors" ADD FOREIGN KEY ("file_id") REFERENCES "files" ("id");