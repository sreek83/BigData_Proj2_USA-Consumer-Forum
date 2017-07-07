REGISTER /usr/local/pig/lib/piggybank.jar;

DEFINE CSVloader org.apache.pig.piggybank.storage.CSVExcelStorage();

file = LOAD '/flume_import/Consumer_Complaints.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage (',') AS (Date_received:chararray, Product:chararray,  Sub_product:chararray, Issue:chararray, Sub_issue:chararray, Consumer_complaint: chararray, Comp_public_response:chararray, Company:chararray,State:chararray,ZIP:chararray, Submitted_via:chararray, Date_sent_to_comp:chararray,Comp_response_to_consumer:chararray,Timely_resp:chararray,
Consumer_disputed:chararray,Complaint_ID:chararray);

group_comp_file = GROUP file BY Company;

new_group_file = FOREACH group_comp_file generate group, COUNT(file);

sorted_group_file = ORDER new_group_file BY $1;

output = FOREACH sorted_group_file generate $0;

STORE output INTO '/user/acadgild/assign2/task3';
 




