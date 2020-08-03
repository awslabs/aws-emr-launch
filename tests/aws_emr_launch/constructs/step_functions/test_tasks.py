from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import core

from aws_emr_launch.constructs.emr_constructs import emr_profile
from aws_emr_launch.constructs.step_functions import emr_tasks


def test_start_execution_task():
    default_task = {
        'End': True,
        'Parameters': {
            'StateMachineArn': {'Ref': 'teststatemachine7F4C511D'},
            'Input.$': '$$.Execution.Input'
        },
        'Type': 'Task',
        'Resource': {
            'Fn::Join': ['', ['arn:', {'Ref': 'AWS::Partition'}, ':states:::states:startExecution.sync']]
        }
    }

    app = core.App()
    stack = core.Stack(app, 'test-stack')

    state_machine = sfn.StateMachine(
        stack, 'test-state-machine',
        definition=sfn.Chain.start(sfn.Succeed(stack, 'Succeeded')))

    task = sfn.Task(
        stack, 'test-task',
        task=emr_tasks.StartExecutionTask(
            state_machine,
        )
    )

    resolved_task = stack.resolve(task.to_state_json())
    print(default_task)
    print(resolved_task)
    assert default_task == resolved_task


def test_start_execution_task_with_input():
    default_task = {
        'End': True,
        'Parameters': {
            'StateMachineArn': {'Ref': 'teststatemachine7F4C511D'},
            'Input': {'Key1': 'Value1'},
            'Name': 'test-sfn-task'
        },
        'Type': 'Task',
        'Resource': {
            'Fn::Join': ['', ['arn:', {'Ref': 'AWS::Partition'}, ':states:::states:startExecution.sync']]
        }
    }

    app = core.App()
    stack = core.Stack(app, 'test-stack')

    state_machine = sfn.StateMachine(
        stack, 'test-state-machine',
        definition=sfn.Chain.start(sfn.Succeed(stack, 'Succeeded')))

    task = sfn.Task(
        stack, 'test-task',
        task=emr_tasks.StartExecutionTask(
            state_machine,
            input={'Key1': 'Value1'},
            name='test-sfn-task'
        )
    )

    resolved_task = stack.resolve(task.to_state_json())
    print(default_task)
    print(resolved_task)
    assert default_task == resolved_task


def test_emr_create_cluster_task():
    default_task = {
        'End': True,
        'Parameters': {
            'AdditionalInfo.$': '$.ClusterConfiguration.Cluster.AdditionalInfo',
            'AmiVersion.$': '$.ClusterConfiguration.Cluster.AmiVersion',
            'Applications.$': '$.ClusterConfiguration.Cluster.Applications',
            'AutoScalingRole.$': '$.ClusterConfiguration.Cluster.AutoScalingRole',
            'BootstrapActions.$': '$.ClusterConfiguration.Cluster.BootstrapActions',
            'Configurations.$': '$.ClusterConfiguration.Cluster.Configurations',
            'CustomAmiId.$': '$.ClusterConfiguration.Cluster.CustomAmiId',
            'EbsRootVolumeSize.$': '$.ClusterConfiguration.Cluster.EbsRootVolumeSize',
            'Instances': {
                'AdditionalMasterSecurityGroups.$':
                    '$.ClusterConfiguration.Cluster.Instances.AdditionalMasterSecurityGroups',
                'AdditionalSlaveSecurityGroups.$':
                    '$.ClusterConfiguration.Cluster.Instances.AdditionalSlaveSecurityGroups',
                'Ec2KeyName.$': '$.ClusterConfiguration.Cluster.Instances.Ec2KeyName',
                'Ec2SubnetId.$': '$.ClusterConfiguration.Cluster.Instances.Ec2SubnetId',
                'Ec2SubnetIds.$': '$.ClusterConfiguration.Cluster.Instances.Ec2SubnetIds',
                'EmrManagedMasterSecurityGroup.$':
                    '$.ClusterConfiguration.Cluster.Instances.EmrManagedMasterSecurityGroup',
                'EmrManagedSlaveSecurityGroup.$':
                    '$.ClusterConfiguration.Cluster.Instances.EmrManagedSlaveSecurityGroup',
                'HadoopVersion.$': '$.ClusterConfiguration.Cluster.Instances.HadoopVersion',
                'InstanceCount.$': '$.ClusterConfiguration.Cluster.Instances.InstanceCount',
                'InstanceFleets.$': '$.ClusterConfiguration.Cluster.Instances.InstanceFleets',
                'InstanceGroups.$': '$.ClusterConfiguration.Cluster.Instances.InstanceGroups',
                'KeepJobFlowAliveWhenNoSteps': True,
                'MasterInstanceType.$': '$.ClusterConfiguration.Cluster.Instances.MasterInstanceType',
                'Placement.$': '$.ClusterConfiguration.Cluster.Instances.Placement',
                'ServiceAccessSecurityGroup.$': '$.ClusterConfiguration.Cluster.Instances.ServiceAccessSecurityGroup',
                'SlaveInstanceType.$': '$.ClusterConfiguration.Cluster.Instances.SlaveInstanceType',
                'TerminationProtected.$': '$.ClusterConfiguration.Cluster.Instances.TerminationProtected'
            },
            'JobFlowRole.$': '$.ClusterConfiguration.Cluster.JobFlowRole',
            'KerberosAttributes.$': '$.ClusterConfiguration.Cluster.KerberosAttributes',
            'LogUri.$': '$.ClusterConfiguration.Cluster.LogUri',
            'Name.$': '$.ClusterConfiguration.Cluster.Name',
            'NewSupportedProducts.$': '$.ClusterConfiguration.Cluster.NewSupportedProducts',
            'ReleaseLabel.$': '$.ClusterConfiguration.Cluster.ReleaseLabel',
            'RepoUpgradeOnBoot.$': '$.ClusterConfiguration.Cluster.RepoUpgradeOnBoot',
            'ScaleDownBehavior.$': '$.ClusterConfiguration.Cluster.ScaleDownBehavior',
            'SecurityConfiguration.$': '$.ClusterConfiguration.Cluster.SecurityConfiguration',
            'ServiceRole.$': '$.ClusterConfiguration.Cluster.ServiceRole',
            'StepConcurrencyLevel.$': '$.ClusterConfiguration.Cluster.StepConcurrencyLevel',
            'SupportedProducts.$': '$.ClusterConfiguration.Cluster.SupportedProducts',
            'Tags.$': '$.ClusterConfiguration.Cluster.Tags',
            'VisibleToAllUsers.$': '$.ClusterConfiguration.Cluster.VisibleToAllUsers'
        },
        'Resource':
            {'Fn::Join': ['', ['arn:', {'Ref': 'AWS::Partition'}, ':states:::elasticmapreduce:createCluster.sync']]
        },
        'Type': 'Task'
    }

    app = core.App()
    stack = core.Stack(app, 'test-stack')

    task = sfn.Task(
        stack, 'test-task',
        task=emr_tasks.EmrCreateClusterTask(
            roles=emr_profile.EMRRoles(stack, 'test-emr-roles', role_name_prefix='test-roles'),
            cluster_configuration_path='$.ClusterConfiguration.Cluster',
        )
    )

    resolved_task = stack.resolve(task.to_state_json())
    print(default_task)
    print(resolved_task)
    assert default_task == resolved_task


def test_emr_add_step_task():
    default_task = {
        'End': True,
        'Parameters': {
            'ClusterId': 'test-cluster-id',
            'Step': {'Key1': {'Key2': 'Value2'}}
        },
        'Resource': {'Fn::Join': ['', ['arn:', {'Ref': 'AWS::Partition'}, ':states:::elasticmapreduce:addStep.sync']]},
        'Type': 'Task'
    }

    app = core.App()
    stack = core.Stack(app, 'test-stack')

    task = sfn.Task(
        stack, 'test-task',
        task=emr_tasks.EmrAddStepTask(
            'test-cluster-id',
            {'Key1': {'Key2': 'Value2'}}
        )
    )

    resolved_task = stack.resolve(task.to_state_json())
    print(default_task)
    print(resolved_task)
    assert default_task == resolved_task
