USE [nse]
GO
/****** Object:  User [nseuser]    Script Date: 11/16/2019 1:37:40 PM ******/
CREATE USER [nseuser] FOR LOGIN [nseuser] WITH DEFAULT_SCHEMA=[dbo]
GO
ALTER ROLE [db_owner] ADD MEMBER [nseuser]
GO
ALTER ROLE [db_securityadmin] ADD MEMBER [nseuser]
GO
ALTER ROLE [db_ddladmin] ADD MEMBER [nseuser]
GO
ALTER ROLE [db_backupoperator] ADD MEMBER [nseuser]
GO
ALTER ROLE [db_datareader] ADD MEMBER [nseuser]
GO
ALTER ROLE [db_datawriter] ADD MEMBER [nseuser]
GO
/****** Object:  Table [dbo].[ExpiryDates]    Script Date: 11/16/2019 1:37:40 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[ExpiryDates](
	[ExpiryDatesID] [int] IDENTITY(1,1) NOT NULL,
	[SymbolID] [int] NULL,
	[ExpiryDates] [varchar](25) NULL,
 CONSTRAINT [pk_expirydates_eid] PRIMARY KEY CLUSTERED 
(
	[ExpiryDatesID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
/****** Object:  Table [dbo].[OptionChainData]    Script Date: 11/16/2019 1:37:40 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[OptionChainData](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[strikePrice] [float] NULL,
	[expiryDate] [datetime] NULL,
	[underlying] [nvarchar](255) NULL,
	[identifier] [nvarchar](255) NULL,
	[openInterest] [float] NULL,
	[changeinOpenInterest] [float] NULL,
	[pchangeinOpenInterest] [float] NULL,
	[totalTradedVolume] [float] NULL,
	[impliedVolatility] [float] NULL,
	[lastPrice] [float] NULL,
	[change] [float] NULL,
	[pChange] [float] NULL,
	[totalBuyQuantity] [float] NULL,
	[totalSellQuantity] [float] NULL,
	[bidQty] [float] NULL,
	[bidprice] [float] NULL,
	[askQty] [float] NULL,
	[askPrice] [float] NULL,
	[underlyingValue] [float] NULL,
	[type] [nvarchar](255) NULL,
	[Time] [float] NULL,
	[datetime] [nvarchar](255) NULL
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[Symbol]    Script Date: 11/16/2019 1:37:40 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[Symbol](
	[SymbolID] [int] IDENTITY(1,1) NOT NULL,
	[SymbolName] [varchar](25) NULL,
 CONSTRAINT [pk_symbol_pid] PRIMARY KEY CLUSTERED 
(
	[SymbolID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[ExpiryDates]  WITH CHECK ADD  CONSTRAINT [fk_expirydates_sid] FOREIGN KEY([SymbolID])
REFERENCES [dbo].[Symbol] ([SymbolID])
GO
ALTER TABLE [dbo].[ExpiryDates] CHECK CONSTRAINT [fk_expirydates_sid]
GO
