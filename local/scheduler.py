# the scheduler is one of the most important parts of any distributed system
# usually a distributed system exposes a language (e.g., hive, sparksql etc. )
# so that the developers define their workflows. The developer's input is parsed and the
# parsed commands is passed to the scheduler. The scheduler's purpose is to "translate"
# the developer's commands into tasks for the task executor and produce the execution plan
# The current system does not exposes a specific language to define the workflows so there is no a parser
# and the developer's commands are passed directly to the scheduler using python generators (yield).
# Most of the time, the scheduler is the most complicated part of a system which may also introduce significant overheads,
# thus it should be as optimal as possible and keep it as simple as possible so that it is fast and extensible.

#import pickle
states = {}

class Scheduler:
    def __init__(self, task_executor, algorithm, step, schema):
        self.task_executor = task_executor
        # if static schema is False the algorithm defines dynamically the returned schema in each step.
        # otherwise the algorithm sets a static schema for local/global tasks and all the next local global tasks follow
        # the defined static schema.
        print("step: "+step)
        self.step = int(step)

        self.static_schema = int(schema)
        self.schema = {}
        self.local_schema = None
        self.global_schema = None
        # the termination condition is processed either in the dbms or in python algorithm's workflow
        # to check the condition in the dbms an additional attribute named `termination` is required in global schema
        self.termination_in_dbms = False

        ## bind parameters before pushing them to the algorithm - necessary step to avoid sql injections
        self.parameters = task_executor.bindparameters(task_executor.parameters)
        self.task_generator = task_executor.create_task_generator(algorithm)

    async def schedule(self):
        if self.step == 0:
            await self.task_executor.createlocalviews()
        else:
            i = 1
            for task in self.task_generator:
                if i == self.step:
                    if 'set_schema' in task:
                        await self.set_schema(task['set_schema'])
                    elif 'define_udf' in task:
                        try:
                            await self.define_udf(task['define_udf'])
                        except:
                            raise Exception('''online UDF definition is not implemented yet''')
                    elif 'run_local' in task:
                        await self.run_local(task['run_local'])

                i+=1

        return 1

    async def set_schema(self, schema):
        self.static_schema = True
        self.schema = schema
        if 'termination' in schema['global']:
            self.termination_in_dbms = True
        await self.task_executor.init_tables(schema['local'], schema['global'])

    async def define_udf(self, udf):
        await self.task_executor.register_udf(udf)

    async def run_local(self, step_local):
        if not self.static_schema and 'schema' not in step_local:
            raise Exception('''Schema definition is missing''')
        #if self.static_schema:
        #    self.local_schema = self.schema['local']
        #else:
        #    self.local_schema = step_local['schema']
        await self.task_executor.task_local(self.local_schema, step_local['sqlscript'])

    async def run_global(self, step_global):
        if not self.static_schema and 'schema' not in step_global:
            raise Exception('''Schema definition is missing''')
        if self.static_schema:
            self.global_schema = self.schema['global']
        else:
            self.global_schema = step_global['schema']
            if 'termination' in self.global_schema:
                self.termination_in_dbms = True

        result = await self.task_executor.task_global(self.global_schema, step_global['sqlscript'])
        return result

    def termination(self, global_result):
        return global_result[len(global_result)-1][0]
