# Hadoop MapReduce Key Salting for Data Skew

## Overview

This project demonstrates how to solve the hot key problem in Hadoop MapReduce using key salting.

A highly frequent customer ID (`CUST_HOT`) creates reducer skew because all transactions for that customer would normally be processed by a single reducer. Key salting distributes the workload across multiple reducers before recombining the results.

## Architecture

### Phase 1: Salt Keys

Mapper:
- Reads customer transactions
- Adds a random salt bucket (0-9)
- Emits salted customer keys

Example:
CUST_HOT_3 -> 1250.50
CUST_HOT_7 -> 980.25

Reducer:
- Calculates partial sums for each salted key

### Phase 2: Remove Salt

Mapper:
- Removes salt suffix

Example:
CUST_HOT_3 -> CUST_HOT

Reducer:
- Combines partial sums into final totals


## Technologies

- Java
- Hadoop MapReduce
- HDFS
- Docker

## Results

Input:
- 6,000 transactions

Phase 1:
- 207 salted intermediate keys

Phase 2:
- 21 final customer totals

Example output:


CUST_HOT 1270743.47


The hot key was distributed across reducers, reducing the impact of data skew.





















