# the scheduler is one of the most important parts of any distributed system
# usually a distributed system exposes a language (e.g., hive, sparksql etc. )
# so that the developers define their workflows. The developer's input is parsed and the
# parsed commands is passed to the scheduler. The scheduler's purpose is to "translate"
# the developer's commands into tasks for the task executor and produce the execution plan
# The current system does not exposes a specific language to define the workflows so there is no a parser
# and the developer's commands are passed directly to the scheduler using python generators (yield).
# Most of the time, the scheduler is the most complicated part of a system which may also introduce significant overheads,
# thus it should be as optimal as possible and keep it as simple as possible so that it is fast and extensible.

states = {}

class Scheduler:
    def __init__(self, task_executor, algorithm, step, schema, states):
        self.task_executor = task_executor
        # if static schema is False the algorithm defines dynamically the returned schema in each step.
        # otherwise the algorithm sets a static schema for local/global tasks and all the next local global tasks follow
        # the defined static schema.
        print("step: "+str(step))
        self.step = int(step)

        print(states)
        self.static_schema = int(schema)
        self.schema = {}
        self.local_schema = None
        self.global_schema = None
        # the termination condition is processed either in the dbms or in python algorithm's workflow
        # to check the condition in the dbms an additional attribute named `termination` is required in global schema
        self.termination_in_dbms = False


        ## bind parameters before pushing them to the algorithm - necessary step to avoid sql injections
        self.parameters = task_executor.bindparameters(task_executor.parameters)

        if 'task_generator' not in states:
            self.task_generator = task_executor.create_task_generator(algorithm)
            states['task_generator'] = self.task_generator
        else:
            self.task_generator = states['task_generator']
        self.states = states
        if 'termination' not in self.states:
            self.states['termination'] = False


    async def schedule(self):
        if self.step == 0:
            await self.task_executor.createlocalviews()
        elif self.step > 0:
            for task in self.task_generator:
                if 'run_global' in task:
                    if 'schema' in task['run_global']:
                        await self.run_global(task['run_global'])
                        
                        if self.states['termination']:
                            pass
                        else:
                            next(self.task_generator)
                            task = self.task_generator.send(await self.task_executor.get_global_result())
                        break
                    if self.states['termination']:
                        pass
                    else:
                        next(self.task_generator)
                        task = self.task_generator.send(await
                        self.task_executor.get_global_result())
                if 'set_schema' in task:
                    await self.set_schema(task['set_schema'])
                    break
                elif 'define_udf' in task:
                    try:
                        await self.define_udf(task['define_udf'])
                    except:
                        raise Exception('''online UDF definition is not implemented yet''')
                    break
                elif 'run_local' in task:
                    #if self.states['termination']:
                    await self.run_local(task['run_local'])
                    break
                    #else:
                    #    await self.run_local(self.task_generator.send(await self.task_executor.get_global_result()))


            self.states['step'] = self.step
        elif self.step == -1: ## cleanup
            await self.task_executor.clean_up()
        return 1



    async def set_schema(self, schema):
        self.schema = schema
        if 'termination' in schema['global']:
            self.termination_in_dbms = True
            self.states['termination'] = True
        else:
            self.termination_in_dbms = False
            self.states['termination'] = False
        await self.task_executor.init_tables(schema['local'], schema['global'])

    async def define_udf(self, udf):
        await self.task_executor.register_udf(udf)

    async def run_local(self, step_local):
        if not self.static_schema and 'schema' not in step_local:
            raise Exception('''Schema definition is missing''')
        if not self.static_schema:
            self.local_schema = step_local['schema']
        await self.task_executor.task_local(self.local_schema, self.static_schema, step_local['sqlscript'])

    async def run_global(self, step_global):
        if not self.static_schema and 'schema' not in step_global:
            raise Exception('''Schema definition is missing''')
        if 'termination' in step_global['schema']:
            self.termination_in_dbms = True
            self.states['termination'] = True
        else:
            self.termination_in_dbms = False
            self.states['termination'] = False
        if not self.static_schema:
            result = await self.task_executor.init_global_remote_table(step_global['schema'])
            return result

    def termination(self, global_result):
        return global_result[len(global_result)-1][0]
