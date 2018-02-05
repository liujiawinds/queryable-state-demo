# queryable-state-demo
flink queryable state demo in version 1.4.0



## tips

1. Add dependecy, `flink-queryable-state-runtime_2.11-1.4.0.jar`
2. Check the logs of any task manager for the line: "Started the Queryable State Proxy Server @ ..."
3. Configure the port and host for the queryable client, the port must be of the "Queryable State Proxy Server" and the host must be one of the TaskManager's
  
  ```
    In 1.3.2 the port and host was the same as flink's JobManager.
  ```
