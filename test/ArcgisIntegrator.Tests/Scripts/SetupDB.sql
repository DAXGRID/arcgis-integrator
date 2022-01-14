CREATE DATABASE SUPERGIS
GO

USE SUPERGIS
GO

CREATE SCHEMA sde;
GO

-- CABLE TABLE
CREATE TABLE [dbo].[cable](
	[OBJECTID] [int] NOT NULL,
	[voltage_level] [int] NULL,
	[psr_type] [varchar](255) NULL,
	[manufactuer_id] [uniqueidentifier] NULL,
	[manufactuer_name] [varchar](255) NULL,
	[product_asset_model_id] [uniqueidentifier] NULL);
GO

-- a524 TABLE
CREATE TABLE [dbo].[a524](
	[OBJECTID] [int] NOT NULL,
	[SDE_STATE_ID] [bigint] NOT NULL,
	[voltage_level] [int] NULL,
	[psr_type] [varchar](255) NULL,
	[manufactuer_id] [uniqueidentifier] NULL,
	[manufactuer_name] [varchar](255) NULL,
	[product_asset_model_id] [uniqueidentifier] NULL);
GO

-- D524 TABLE
CREATE TABLE [dbo].[D524](
	[SDE_STATE_ID] [bigint] NOT NULL,
	[SDE_DELETES_ROW_ID] [int] NOT NULL,
	[DELETED_AT] [bigint] NOT NULL);
GO

-- VERSIONS TABLE
CREATE TABLE [sde].[SDE_versions](
	[name] [varchar](64) NOT NULL,
	[owner] [varchar](32) NOT NULL,
	[version_id] [int] NOT NULL,
	[status] [int] NOT NULL,
	[state_id] [bigint] NOT NULL,
	[description] [varchar](64) NULL,
	[parent_name] [varchar](64) NULL,
	[parent_owner] [varchar](32) NULL,
	[parent_version_id] [int] NULL,
	[creation_time] [datetime] NOT NULL);
GO

-- Enable CDC on Database
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on versions table
EXECUTE sys.sp_cdc_enable_table
    @source_schema = N'sde'
  , @source_name = N'SDE_versions'
  , @role_name = N'null';
GO

INSERT INTO SUPERGIS.sde.SDE_versions
(name, owner, version_id, status, state_id, description, parent_name, parent_owner, parent_version_id, creation_time)
VALUES('DEFAULT', 'sde', 1, 1, 0, 'Instance default version', null, null, null, '2022-01-01');
GO
