
# Data Warehouse Export

## Current Inventory
| product_id | warehouse   | quantity | last_audit |
|:-----------|:------------|---------:|:-----------|
| 50         | North_WH    | 120      | 2023-10-01 |
| 50         | South_WH    | 45       | 2023-10-02 |
| 51         | North_WH    | 500      | 2023-09-15 |
| 52         | East_WH     | 30       | 2023-10-05 |

## Recent Events
| event_id | event_type | severity | description       |
|----------|------------|----------|-------------------|
| 1001     | ERROR      | High     | Connection timeout|
| 1002     | WARNING    | Medium   | Disk usage > 80%  |
| 1003     | INFO       | Low      | Job completed     |
