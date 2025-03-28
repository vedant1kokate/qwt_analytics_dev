{{config(materialized = 'table')}}
 
select 
OrderID	as OrderID,
LineNo	as LineNo,
ShipperID	as ShipperID,
CustomerID	as CustomerID,
ProductID	as ProductID,
EmployeeID	as EmployeeID,
SPLIT_PART(ShipmentDate, ' ',1)::date as ShipmentDate,
Status as Status
from
{{source('qwt_raw','raw_shipments')}}