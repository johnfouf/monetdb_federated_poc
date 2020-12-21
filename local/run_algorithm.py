import datetime
import random
import task_executor
import transfer
import json
import importlib
import settings
import scheduler

DEBUG = settings.DEBUG


async def run(algorithm_instance, params, hash_value, step, schema,node_id, db_objects, states):
    ### get the corresponding algorithm python module using algorithm name
    table_id = hash_value
    transfer_runner = transfer.Transfer(db_objects, table_id)
    task_executor_instance = task_executor.Task(db_objects, table_id, params, node_id, transfer_runner)
    scheduler_instance  = scheduler.Scheduler(task_executor_instance, algorithm_instance.algorithm, step, schema, states)
    result = await scheduler_instance.schedule()
    return result