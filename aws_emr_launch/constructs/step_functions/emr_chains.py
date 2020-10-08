from typing import List, Mapping, Optional

from aws_cdk import aws_sns as sns
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_code
from aws_emr_launch.constructs.lambdas import emr_lambdas
from aws_emr_launch.constructs.step_functions import emr_tasks


class Success(sfn.StateMachineFragment):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None,
                 result_path: str = '$.PublishResult', output_path: Optional[str] = None):
        super().__init__(scope, id)

        self._end = sfn.Succeed(
            self, 'Succeeded', output_path=output_path
        )

        if topic is not None:
            self._start = sfn_tasks.SnsPublish(
                self, 'Success Notification',
                input_path='$',
                output_path='$',
                result_path=result_path,
                topic=topic,
                message=message,
                subject=subject,
            )
            self._start.next(self._end)
        else:
            self._start = self._end

    @property
    def start_state(self) -> sfn.State:
        return self._start

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self._end.end_states


class Fail(sfn.StateMachineFragment):
    def __init__(self, scope: core.Construct, id: str, *,
                 message: sfn.TaskInput, subject: Optional[str] = None,
                 topic: Optional[sns.Topic] = None,
                 result_path: str = '$.PublishResult', output_path: str = '$',
                 cause: Optional[str] = None, comment: Optional[str] = None,
                 error: Optional[str] = None):
        super().__init__(scope, id)

        self._end = sfn.Fail(
            self, 'Execution Failed', cause=cause, comment=comment, error=error
        )

        if topic is not None:
            self._start = sfn_tasks.SnsPublish(
                self, 'Failure Notification',
                input_path='$',
                output_path=output_path,
                result_path=result_path,
                topic=topic,
                message=message,
                subject=subject,
            )
            self._start.next(self._end)
        else:
            self._start = self._end

    @property
    def start_state(self) -> sfn.State:
        return self._start

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self._end.end_states


class NestedStateMachine(sfn.StateMachineFragment):
    def __init__(self, scope: core.Construct, id: str, name: str, state_machine: sfn.StateMachine,
                 input: Optional[Mapping[str, any]] = None, fail_chain: Optional[sfn.IChainable] = None):
        super().__init__(scope, id)

        state_machine_task = emr_tasks.StartExecutionTask(
            self, name,
            state_machine=state_machine,
            input=input,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        parse_json_string = emr_lambdas.ParseJsonStringBuilder.get_or_build(self)

        parse_json_string_task = sfn_tasks.LambdaInvoke(
            self, f'{name} - Parse JSON Output',
            result_path='$',
            lambda_function=parse_json_string,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object({
                'JsonString': sfn.TaskInput.from_data_at('$.Output').value
            }),
        )

        if fail_chain:
            state_machine_task.add_catch(fail_chain, errors=['States.ALL'], result_path='$.Error')
            parse_json_string_task.add_catch(fail_chain, errors=['States.ALL'], result_path='$.Error')

        state_machine_task.next(parse_json_string_task)

        self._start = state_machine_task
        self._end = parse_json_string_task

    @property
    def start_state(self) -> sfn.State:
        return self._start

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self._end.end_states


class AddStepWithArgumentOverrides(sfn.StateMachineFragment):
    def __init__(self, scope: core.Construct, id: str, *,
                 emr_step: emr_code.EMRStep,
                 cluster_id: str,
                 result_path: Optional[str] = None,
                 output_path: Optional[str] = None,
                 fail_chain: Optional[sfn.IChainable] = None,
                 wait_for_step_completion: bool = True):
        super().__init__(scope, id)

        override_step_args = emr_lambdas.OverrideStepArgsBuilder.get_or_build(self)

        override_step_args_task = sfn_tasks.LambdaInvoke(
            self, f'{emr_step.name} - Override Args',
            result_path=f'$.{id}ResultArgs',
            lambda_function=override_step_args,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object({
                'ExecutionInput': sfn.TaskInput.from_context_at('$$.Execution.Input').value,
                'StepName': emr_step.name,
                'Args': emr_step.args
            }),
        )

        resolved_step = emr_step.resolve(self)
        resolved_step['HadoopJarStep']['Args'] = sfn.TaskInput.from_data_at(f'$.{id}ResultArgs').value

        integration_pattern = sfn.IntegrationPattern.RUN_JOB if wait_for_step_completion \
            else sfn.IntegrationPattern.REQUEST_RESPONSE

        add_step_task = emr_tasks.EmrAddStepTask(
            self, emr_step.name,
            output_path=output_path,
            result_path=result_path,
            cluster_id=cluster_id,
            step=resolved_step,
            integration_pattern=integration_pattern,
        )

        if fail_chain:
            override_step_args_task.add_catch(fail_chain, errors=['States.ALL'], result_path='$.Error')
            add_step_task.add_catch(fail_chain, errors=['States.ALL'], result_path='$.Error')

        override_step_args_task.next(add_step_task)

        self._start = override_step_args_task
        self._end = add_step_task

    @property
    def start_state(self) -> sfn.State:
        return self._start

    @property
    def end_states(self) -> List[sfn.INextable]:
        return self._end.end_states
