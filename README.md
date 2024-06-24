# DATA ENGINEERING ASSESSMENT

You have been assigned the responsibility of assisting Qualifyze's Data Team in the analysis of some business data. You have been provided with two files for this purpose:

* [orders.csv](resources/orders.csv) (which contains factual information regarding the orders received)
* [invoicing_data.json](resources/invoicing_data.json) (which contains invoicing information)

Explore the raw data and provide the code (well commented) to answer the following questions/scenarios. For this exercise you can only use Python (or PySpark):

## Kata 1

Distribution of Audit Type per Company (# of Orders per Type)

## Kata 2

Provide a DF (df_1) containing the following columns:

| Column            | Description                                                                                                                                                                |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| order_id          | The order_id field must contain the unique identifier of the order.                                                                                                        |
| auditor_full_name | The auditor_full_name field must contain the complete name of the auditor. In case this information is not available, the placeholder "Hans Zimmermann" should be utilized |

## Kata 3

Provide a DF (df_2) containing the following columns:

| Column          | Description                                                                                                                                                                                                                                                                                    |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| order_id        | The order_id field must contain the unique identifier of the order.                                                                                                                                                                                                                            |
| auditor_address | The field for auditor_address should adhere to the following information and format: "city name, postal code". In the event that the city name is not available, the placeholder "Unknown" should be used. Similarly, if the postal code is not known, the placeholder "UNK00" should be used. |

## Kata 4

The Sales Team requires your assistance in computing the commissions. It is possible for multiple salespersons to be associated with a single order, as they may have participated in different stages of the order. The "salesowners" field comprises a ranked list of the salespeople who have ownership of the order. The first individual on the list represents the primary owner, while the subsequent individuals, if any, are considered co-owners who have contributed to the acquisition process. The calculation of commissions follows a specific procedure:

• Main Owner: 6% of the net invoiced value
• Co-owner 1 (second in list): 2,5% of the net invoiced value
• Co-owner 2 (third in list): 0,95% of the new invoiced value
• The rest of the Co-owners do not receive anything.

Provide a list of the distinct sales owners and their respective commission earnings. The list should be sorted in order of descending performance, with the sales owners who have generated the highest commissions appearing first.

HINT: Raw Amounts are represented in cents. Please provide euro amounts with two decimal places in the results.

## Kata 5

Provide a DF (df_3) containing the following columns:

| company_id       | The company_id field must contain the unique identifier of the company. |
| company_name     | The company_name field must contain the name of the company |
| list_salesowners | The "list _salesowners" field should contain a unique and comma-separated list of salespeople who have participated in at least one order of the company. Please ensure that the list is sorted in ascending alphabetical order of the first name. |

**HINT:** Please consider the possibility of duplicate companies stored under multiple IDs in the database. Take this into account while devising a solution to this exercise.