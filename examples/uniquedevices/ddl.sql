-- reset if running again
DROP PROCEDURE CountDeviceEstimate IF EXISTS;
DROP PROCEDURE CountDeviceExact IF EXISTS;
DROP PROCEDURE CountDeviceHybrid IF EXISTS;
DROP PROCEDURE GetCardEstForApp IF EXISTS;
DROP PROCEDURE TopApps IF EXISTS;
DROP TABLE estimates IF EXISTS;
DROP TABLE exact IF EXISTS;

-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE estimates
(
  appid       bigint          NOT NULL,
  devicecount bigint          NOT NULL,
  hll         varbinary(8192) DEFAULT NULL,
  CONSTRAINT  PK_estimates PRIMARY KEY (appid)
);
PARTITION TABLE estimates ON COLUMN appid;
CREATE INDEX rank ON ESTIMATES (devicecount);

CREATE TABLE exact
(
  appid      bigint          NOT NULL,
  deviceid   bigint          NOT NULL,
  CONSTRAINT PK_exact PRIMARY KEY (appid, deviceid)
);
PARTITION TABLE exact ON COLUMN appid;

-- load classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES uniquedevices-procs.jar;

-- stored procedures

CREATE PROCEDURE FROM CLASS uniquedevices.CountDeviceEstimate;
PARTITION PROCEDURE CountDeviceEstimate ON TABLE estimates COLUMN appid;
CREATE PROCEDURE FROM CLASS uniquedevices.CountDeviceExact;
PARTITION PROCEDURE CountDeviceExact ON TABLE estimates COLUMN appid;
CREATE PROCEDURE FROM CLASS uniquedevices.CountDeviceHybrid;
PARTITION PROCEDURE CountDeviceHybrid ON TABLE estimates COLUMN appid;

CREATE PROCEDURE uniquedevices.GetCardEstForApp AS
    SELECT devicecount FROM estimates WHERE appid = ?;
PARTITION PROCEDURE GetCardEstForApp ON TABLE estimates COLUMN appid;
CREATE PROCEDURE uniquedevices.TopApps AS
    SELECT appid, devicecount FROM estimates ORDER BY devicecount DESC LIMIT 10;