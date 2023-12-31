-- -----------------------------------------------------------------------
-- ------------------------ UPLOADING THE DATA ---------------------------
-- -----------------------------------------------------------------------

-- Create a database to load the data into.

CREATE DATABASE HelpDesk;
USE HelpDesk;

-- Create a table to import the csv file into.

CREATE TABLE tickets(ticket_number CHAR(30),
			first_name CHAR(30),
			last_name CHAR(30),
			owner_group CHAR(30),
			issue_category CHAR(30),
			ticket_type CHAR(10),
			severity CHAR(20),
			days_open integer,
			satisfaction_score CHAR(30),
			ticket_status CHAR(30),
			created_date VARCHAR(10) 
	 		);
                    
-- Here I used table data import wizard to load the data in.				

                    
-- -----------------------------------------------------------------------
-- ------------------------- CLEANING THE DATA ---------------------------
-- ----------------------------------------------------------------------- 
                    
-- Setting sql_safe_update to zero allows you to alter the table.

SET SQL_SAFE_UPDATES = 0;

-- The created_date is a string, so we need to convert it to a date.

UPDATE tickets SET created_date = STR_TO_DATE(created_date, "%m/%d/%Y");

SELECT * FROM tickets;
					
-- We want to use only the number from the severity ranking.		
		
UPDATE tickets SET severity = LEFT(severity, 1);

-- We want to use only the number from the satisfaction_score.

UPDATE tickets SET satisfaction_score = LEFT(satisfaction_score, 1);

-- Both of the above columns were strings, so we need to convert them to integers.

ALTER TABLE tickets MODIFY COLUMN severity INTEGER,
			MODIFY COLUMN satisfaction_score INTEGER;

-- This prevents you from altering the table.

SET SQL_SAFE_UPDATES = 1;


-- -----------------------------------------------------------------------
-- ------------------------- EXPLORING THE DATA --------------------------
-- -----------------------------------------------------------------------

-- Finding the number of rows and columns in the data.

SELECT COUNT(*) AS rws FROM tickets;

SELECT COUNT(*) AS columns
FROM information_schema.columns
WHERE table_name = 'tickets';

-- Checking for duplicate ticket numbers.

SELECT COUNT(DISTINCT ticket_number) AS distinct_ticket_nums FROM tickets;

-- Checking the distinct values of columns.

SELECT DISTINCT owner_group FROM tickets;

SELECT DISTINCT issue_category FROM tickets;

SELECT DISTINCT ticket_type FROM tickets;

SELECT DISTINCT severity FROM tickets;

SELECT DISTINCT satisfaction_score FROM tickets;

SELECT DISTINCT ticket_status FROM tickets;

-- Checking the shortest a ticket was open and the longest a ticket was open.

SELECT MAX(days_open) AS longest_open_ticket FROM tickets;

SELECT MIN(days_open) AS shortest_open_ticket FROM tickets;

-- Checking the date range in which these tickets were submitted.

SELECT MIN(created_date) AS earliest_ticket FROM tickets;

SELECT MAX(created_date) AS latest_ticket FROM tickets;

-- Calculating the average time a ticket is open.

SELECT ROUND(AVG(days_open),0) AS avg_open_ticket FROM tickets;


-- AGGREGATIONS:

-- How many tickets were request tickets and how many were issue tickets.

SELECT ticket_type,
	COUNT(ticket_type) AS num_tickets
FROM tickets
GROUP BY ticket_type;

-- Distribution of tickets among sectors.

SELECT owner_group,
	COUNT(*),
	ROUND((COUNT(*)/(SELECT COUNT(*) FROM tickets))*100, 1) AS percentage
FROM tickets
GROUP BY owner_group
ORDER BY percentage DESC;

-- Current ticket status.

SELECT ticket_status,
	COUNT(*) AS num_tickets,
	ROUND((COUNT(*)/(SELECT COUNT(*) FROM tickets))*100, 1) AS percentage
FROM tickets
GROUP BY ticket_status;

-- Satisfaction scores.

SELECT satisfaction_score,
	COUNT(*),
	ROUND((COUNT(*)/(SELECT COUNT(*) FROM tickets))*100, 1) AS percentage
FROM tickets
GROUP BY satisfaction_score
ORDER BY satisfaction_score;

-- Severity levels of the tickets.

SELECT severity,
	COUNT(*),
	ROUND((COUNT(*)/(SELECT COUNT(*) FROM tickets))*100, 1) AS percentage
FROM tickets
GROUP BY severity
ORDER BY severity;

-- Which day of the week saw the most tickets created.

SELECT WEEKDAY(created_date) AS day_of_week,
	COUNT(*) AS num_tickets
FROM tickets
GROUP BY day_of_week
ORDER BY num_tickets;

-- Which month saw the most tickets created.

SELECT MONTH(created_date) AS month,
	COUNT(*) AS num_tickets
FROM tickets
GROUP BY month
ORDER BY num_tickets;
