CREATE DATABASE DATA1
GO

USE DATA1
GO

CREATE SCHEMA dataadmin;
GO

CREATE SCHEMA sde;
GO

-- KABEL TABLE
CREATE TABLE [dataadmin].[KABEL](
	[OBJECTID] [int] NOT NULL,
	[KOMPUNDERSKABID] [numeric](12, 0) NULL,
	[KOMPUNDERSTATIONID] [numeric](12, 0) NULL,
	[SUBTYPECD] [smallint] NULL,
	[DRIFTSANSVARLIG] [varchar](30) NULL,
	[FABRIKAT] [varchar](40) NULL,
	[SPAENDINGSNIVEAU] [varchar](30) NULL,
	[A_TYPE] [varchar](50) NULL,
	[TYPE] [varchar](50) NULL,
	[LIVSCYKLUS] [varchar](20) NULL,
	[A_NEDLAEGNINGSMETODE] [varchar](50) NULL,
	[TROMLESERIENUMMER] [varchar](40) NULL,
	[EJER] [varchar](40) NULL,
	[FUNKTION] [varchar](50) NULL,
	[ANTALFASERPRKABEL] [smallint] NULL,
	[ANTALKABLER] [smallint] NULL,
	[BEMAERKNING1] [varchar](100) NULL,
	[BEMAERKNING2] [varchar](100) NULL,
	[STRAEKNINGOBJEKTID] [int] NULL,
	[STRAEKNINGSID] [varchar](70) NULL,
	[UDFOERINGSNR] [varchar](40) NULL,
	[ID] [varchar](30) NULL,
	[KOMPUNDERKNUDEOBJEKTID] [int] NULL,
	[KNUDEKLASSENAVN] [varchar](100) NULL,
	[FEEDERID] [nvarchar](80) NULL,
	[FEEDERID2] [nvarchar](80) NULL,
	[FEEDERINFO] [int] NULL,
	[MMELECTRICTRACEWEIGHT] [int] NULL,
	[PHASEDESIGNATION] [int] NULL,
	[ANNOFCID] [varchar](2000) NULL,
	[FORS_OMR] [varchar](10) NULL,
	[Ny_RedigeretDato] [datetime] NULL,
	[Ny_RedigeretAf] [varchar](255) NULL,
	[Ny_OprettetDato] [datetime] NULL,
	[Ny_OprettetAf] [varchar](255) NULL,
	[VisuelBryderStatus] [numeric](38, 8) NULL,
	[Enabled] [smallint] NULL,
	[IndlaegningsMetode] [varchar](50) NULL,
	[FdrMgrNonTraceable] [int] NULL,
	[Etableringsdato_Ny] [datetime] NULL,
	[REDUCERET_NUL] [varchar](5) NULL,
	[GlobalID] [uniqueidentifier] NOT NULL DEFAULT ('{00000000-0000-0000-0000-000000000000}'),
	[SHAPE] [geometry] NULL,
	[OPM_DATE] [datetime2](7) NULL,
	[OPM_USER] [nvarchar](50) NULL,
	[FASER] [nvarchar](10) NULL,
 CONSTRAINT [R524_pk] PRIMARY KEY CLUSTERED
(
	[OBJECTID] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

CREATE TABLE [dataadmin].[a524](
	[OBJECTID] [int] NOT NULL,
	[KOMPUNDERSKABID] [numeric](12, 0) NULL,
	[KOMPUNDERSTATIONID] [numeric](12, 0) NULL,
	[SUBTYPECD] [smallint] NULL,
	[DRIFTSANSVARLIG] [varchar](30) NULL,
	[FABRIKAT] [varchar](40) NULL,
	[SPAENDINGSNIVEAU] [varchar](30) NULL,
	[A_TYPE] [varchar](50) NULL,
	[TYPE] [varchar](50) NULL,
	[LIVSCYKLUS] [varchar](20) NULL,
	[A_NEDLAEGNINGSMETODE] [varchar](50) NULL,
	[TROMLESERIENUMMER] [varchar](40) NULL,
	[EJER] [varchar](40) NULL,
	[FUNKTION] [varchar](50) NULL,
	[ANTALFASERPRKABEL] [smallint] NULL,
	[ANTALKABLER] [smallint] NULL,
	[BEMAERKNING1] [varchar](100) NULL,
	[BEMAERKNING2] [varchar](100) NULL,
	[STRAEKNINGOBJEKTID] [int] NULL,
	[STRAEKNINGSID] [varchar](70) NULL,
	[UDFOERINGSNR] [varchar](40) NULL,
	[ID] [varchar](30) NULL,
	[KOMPUNDERKNUDEOBJEKTID] [int] NULL,
	[KNUDEKLASSENAVN] [varchar](100) NULL,
	[FEEDERID] [nvarchar](80) NULL,
	[FEEDERID2] [nvarchar](80) NULL,
	[FEEDERINFO] [int] NULL,
	[MMELECTRICTRACEWEIGHT] [int] NULL,
	[PHASEDESIGNATION] [int] NULL,
	[ANNOFCID] [varchar](2000) NULL,
	[FORS_OMR] [varchar](10) NULL,
	[Ny_RedigeretDato] [datetime] NULL,
	[Ny_RedigeretAf] [varchar](255) NULL,
	[Ny_OprettetDato] [datetime] NULL,
	[Ny_OprettetAf] [varchar](255) NULL,
	[VisuelBryderStatus] [numeric](38, 8) NULL,
	[Enabled] [smallint] NULL,
	[IndlaegningsMetode] [varchar](50) NULL,
	[FdrMgrNonTraceable] [int] NULL,
	[Etableringsdato_Ny] [datetime] NULL,
	[REDUCERET_NUL] [varchar](5) NULL,
	[SDE_STATE_ID] [bigint] NOT NULL,
	[GlobalID] [uniqueidentifier] NOT NULL DEFAULT ('{00000000-0000-0000-0000-000000000000}'),
	[SHAPE] [geometry] NULL,
	[OPM_DATE] [datetime2](7) NULL,
	[OPM_USER] [nvarchar](50) NULL,
	[FASER] [nvarchar](10) NULL,
 CONSTRAINT [a524_rowid_ix1] PRIMARY KEY CLUSTERED
(
	[OBJECTID] ASC,
	[SDE_STATE_ID] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

-- D524 TABLE
CREATE TABLE [dataadmin].[D524](
	[SDE_STATE_ID] [bigint] NOT NULL,
	[SDE_DELETES_ROW_ID] [int] NOT NULL,
	[DELETED_AT] [bigint] NOT NULL,
 CONSTRAINT [d524_pk] PRIMARY KEY NONCLUSTERED
(
	[SDE_STATE_ID] ASC,
	[SDE_DELETES_ROW_ID] ASC,
	[DELETED_AT] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75) ON [PRIMARY]
) ON [PRIMARY];
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
	[creation_time] [datetime] NOT NULL,
 CONSTRAINT [versions_pk] PRIMARY KEY CLUSTERED
(
	[version_id] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY],
 CONSTRAINT [versions_uk] UNIQUE NONCLUSTERED
(
	[name] ASC,
	[owner] ASC
) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY];
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

INSERT INTO DATA1.sde.SDE_versions
(name, owner, version_id, status, state_id, description, parent_name, parent_owner, parent_version_id, creation_time)
VALUES('DEFAULT', 'sde', 1, 1, 0, 'Instance default version', null, null, null, '2022-01-01');
GO
