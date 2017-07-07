
REGISTER /usr/local/pig/lib/piggybank.jar;

file = LOAD '/flume_import/Consumer_Complaints.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage (',') AS (Date_received:chararray, Product:chararray,  Sub_product:chararray, Issue:chararray, Sub_issue:chararray, Consumer_complaint: chararray, Comp_public_response:chararray, Company:chararray,State:chararray,ZIP:chararray, Submitted_via:chararray, Date_sent_to_comp:chararray,Comp_response_to_consumer:chararray,Timely_resp:chararray,
Consumer_disputed:chararray,Complaint_ID:chararray);

filtered_file = FILTER file BY Timely_resp=='Yes';

Grouped_file_all = GROUP filtered_file ALL; 

Grouped_Count = FOREACH Grouped_file_all GENERATE COUNT(filtered_file);

dump Grouped_Count;

