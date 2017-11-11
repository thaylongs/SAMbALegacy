DROP KEYSPACE IF EXISTS dfanalyzer;
CREATE KEYSPACE dfanalyzer WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE dfanalyzer."Execution" (
	id uuid,
	"AplicationName" text,
	"EndTime" timestamp,
	"StartTime" timestamp,
	PRIMARY KEY (id)
);

CREATE TABLE dfanalyzer."TransformationGroup" (
	"executionID" uuid,
	id uuid,
    	"initTasksIDS" SET<uuid>,
    	"intermediaryTasksIDS"  SET<uuid>,
	"finishTaskID" uuid,
	PRIMARY KEY (("executionID"), id)
);

CREATE TABLE dfanalyzer."Task" (
	"executionID" uuid,
	id uuid,
	description text,
	"transformationType" text,
	"schemaFields" LIST<text>,
	PRIMARY KEY ("executionID", id)
);

CREATE TABLE dfanalyzer."DataElement" (
	"executionID" uuid,
	id uuid,
	value LIST<text>,
	PRIMARY KEY (("executionID"), id)
);

CREATE TABLE dfanalyzer."DependenciesOfTask" (
	"executionID" uuid,
	target uuid,
	source SET<uuid>,
	PRIMARY KEY (("executionID"), target)
);

CREATE TABLE dfanalyzer."DependenciesOfDataElement" (
	task uuid,
	source SET<uuid>,
	"executionID" uuid,
	target uuid,
	PRIMARY KEY ("executionID", task, target)
) ;

TRUNCATE dfanalyzer."DataElement";
TRUNCATE dfanalyzer."DependenciesOfDataElement";
TRUNCATE dfanalyzer."Execution";
TRUNCATE dfanalyzer."Task";
TRUNCATE dfanalyzer."DependenciesOfTask";
TRUNCATE dfanalyzer."TransformationGroup";