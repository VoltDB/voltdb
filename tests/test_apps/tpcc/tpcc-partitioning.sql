partition table WAREHOUSE on column W_ID;
partition table DISTRICT on column D_W_ID;
partition table CUSTOMER on column C_W_ID;
partition table HISTORY on column H_W_ID;
partition table STOCK on column S_W_ID;
partition table ORDERS on column O_W_ID;
partition table NEW_ORDER on column NO_W_ID;
partition table ORDER_LINE on column OL_W_ID;

-- Single-partition procedures
create procedure partition on table warehouse columm w_id from class com.procedures.LoadWarehouse;
create procedure partition on table warehouse column w_id from class com.procedures.LoadWarehouseReplicated;
create procedure partition on table warehouse column w_id from class com.procedures.ostatByCustomerId;
create procedure partition on table warehouse column w_id from class com.procedures.delivery;
create procedure partition on table warehouse column w_id from class com.procedures.paymentByCustomerNameW;
create procedure partition on table warehouse column w_id from class com.procedures.paymentByCustomerIdW;
create procedure partition on table warehouse column w_id from class com.procedures.neworder;
create procedure partition on table warehouse column w_id from class com.procedures.slev;
create procedure partition on table warehouse column w_id from class com.procedures.ResetWarehouse;
create procedure partition on table warehouse column w_id from class com.procedures.ostatByCustomerName;
create procedure partition on table customer column c_w_id parameter 3 from class com.procedures.paymentByCustomerNameC;
create procedure partition on table customer column c_w_id parameter 3 from class com.procedures.paymentByCustomerIdC;

-- Multi-partition procedures
create procedure from class com.procedures.paymentByCustomerName;
create procedure from class com.procedures.paymentByCustomerId;
create procedure from class com.procedures.LoadStatus;
