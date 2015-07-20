# This Is Milestone 2.
# 1. Two Numeric Types: Bigint And Integer.
# 2. We Can Create Tables And Insert Into Them.

Create Table Alpha (
  Id Bigint
);

Create Table Beta (
  Id Bigint,
  Local Integer
);

Insert Into Alpha (Id) Values (1), (2), (3);

Insert Into Beta (Local, Id) Values (1, 100), (2, 200), (3, 300);
