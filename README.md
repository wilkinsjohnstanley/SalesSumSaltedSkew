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
