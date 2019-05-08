import unittest
import time
from mock import Mock
import fakeredis
from tasks import HippoTask
from queues import HippoQueue
from addict import Dict
from scheduler import HippoScheduler
from data_sources.hippo_base import HippoDataSource


class TestTask(unittest.TestCase):
    def setUp(self):
        self.redis_client = fakeredis.FakeStrictRedis()

    def tearDown(self):
        self.redis_client.flushall()

    def test_init(self):
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        self.assertIsNotNone(x.mesos_id)
        self.assertIn('task_retries',x.definition)
        self.assertIn('system_retries',x.definition)
        self.assertIn('mesos_id',x.definition)
        y = HippoTask(mesos_id=x.mesos_id,redis_client=self.redis_client)
        self.assertIn('id',y.definition)
        self.assertIn('task_retries',y.definition)
        self.assertIn('system_retries',y.definition)

    def test_validate(self):
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        errors = x.validate()
        self.assertIsNotNone(errors)
        x.definition = {'mem': 32, 'cmd': "echo 'foo'", 'container': {'docker': {'image': 'busybox:latest'}}, 'id': 'fooface', 'cpus': 0.1}
        errors = x.validate()
        self.assertIsNone(errors)

    def test_task_lists(self):
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        x.queue()
        self.assertEqual(len(HippoTask.all_tasks(self.redis_client)),1)
        self.assertEqual(len(HippoTask.waiting_tasks(self.redis_client)),1)
        x.work()
        self.assertEqual(len(HippoTask.working_tasks(self.redis_client)),1)
        self.assertEqual(len(HippoTask.waiting_tasks(self.redis_client)),0)
        x.finish()
        self.assertEqual(len(HippoTask.working_tasks(self.redis_client)),0)
        x.delete()
        self.assertEqual(len(HippoTask.all_tasks(self.redis_client)),0)

    def test_retry(self):
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        x.queue()
        x.work()
        x.definition['task_retries'] = 1
        x.definition['mesos_state'] = 'TASK_FAILED'
        x.finish()
        x.retry()
        self.assertEqual(len(HippoTask.all_tasks(self.redis_client)),2)
        waiting_tasks = HippoTask.waiting_tasks(self.redis_client)
        self.assertEqual(len(waiting_tasks),1)
        w = waiting_tasks[0]
        self.assertEqual(w.definition['task_retries'],0)
        w.finish()
        w.definition['mesos_state'] = 'TASK_LOST'
        w.retry()
        self.assertEqual(len(HippoTask.waiting_tasks(self.redis_client)),2)

    def test_mesos_launch_definition(self):
        x = HippoTask(definition={'mem': 32, 'cmd': "echo 'foo'", 'container': {'docker': {'image': 'busybox:latest'}},
                                  'id': 'fooface', 'cpus': 0.1},
                      redis_client=self.redis_client)
        offer = Dict()
        offer.agent_id.value = '999'
        ld = x.mesos_launch_definition(offer)
        self.assertEqual(ld['agent_id'], {'value': '999'})
        self.assertTrue(hasattr(ld.command.environment, 'variables'))
        self.assertTrue({'name': 'HIPPO_TASK_NAME', 'value': 'fooface'} in ld.command.environment.variables)

    def test_constraints_ok(self):
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        x.definition['constraints'] = [['foo','EQUAL','bar']]
        offer1 = {'attributes':[{'name':'foo','text':'bar'}]}
        offer2 = {'attributes':[{'name':'foo','text':'baz'}]}
        self.assertTrue(x.constraints_ok(offer1))
        self.assertFalse(x.constraints_ok(offer2))
        x.definition['constraints'] = [['foo','UNLIKE','bar']]
        self.assertFalse(x.constraints_ok(offer1))
        self.assertTrue(x.constraints_ok(offer2))


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.redis_client = fakeredis.FakeStrictRedis()

    def tearDown(self):
        self.redis_client.flushall()

    def test_create(self):
        q = HippoQueue(definition={'id':'foo','queue':{'name':'fooname'}},redis_client=self.redis_client)
        self.assertEqual(len(HippoQueue.all_queues(self.redis_client)),1)
        q2 = HippoQueue(id='foo',redis_client=self.redis_client)
        self.assertEqual(q2.definition['queue']['name'],'fooname')
        self.assertEqual(len(HippoQueue.all_queues(self.redis_client)),1)

    def test_processing(self):
        q = HippoQueue(definition={'mem': 32, 'cmd': "echo 'foo'",
                                   'container': {'docker': {'image': 'busybox:latest'}},
                                    'id': 'fooface', 'cpus': 0.1, 'queue':{
                                        'name':'fooname','type':'test'}},
                       redis_client=self.redis_client)
        HippoQueue.process_queues(self.redis_client)
        time.sleep(1)
        HippoQueue.__stop_processing=True

    def test_validate(self):
        q = HippoQueue(definition={'id':'foo','queue':{'name':'fooname','type':'footype'}},redis_client=self.redis_client)
        errors = q.validate()
        self.assertIsNotNone(errors)
        q.definition = {'mem': 32, 'cmd': "echo 'foo'", 'container': {'docker': {'image': 'busybox:latest'}},
                        'id': 'fooface', 'cpus': 0.1, 'queue':{'name':'fooname','type':'redisqueue'}}
        errors = q.validate()
        self.assertIsNone(errors)


class TestHippoBase(unittest.TestCase):
    def setUp(self):
        self.redis_client = fakeredis.FakeStrictRedis()

    def tearDown(self):
        self.redis_client.flushall()

    def test_hippodatasource(self):
        q = HippoQueue(definition={'id':'foo','cmd':'echo $HIPPO_DATA','env':{'foo':'$HIPPO_DATA_BASE64'},
                                   'queue':{'name':'fooname','last_run_tstamp':time.time(),'frequency_seconds':1}},
                       redis_client=self.redis_client)

        hds = HippoDataSource(q,0,HippoTask,self.redis_client)
        self.assertTrue(hds.too_soon())
        hds.process_source()

        hds.create_tasks(['footask1','footask2'])
        waiting_tasks = HippoTask.waiting_tasks(self.redis_client)
        self.assertEqual(len(waiting_tasks),2)
        self.assertEqual(waiting_tasks[0].definition['cmd'],'echo footask2')


class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.redis_client = fakeredis.FakeStrictRedis()

    def tearDown(self):
        self.redis_client.flushall()

    def test_resourceOffers(self):
        driver = Mock()
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        x.queue()
        x.work()
        scheduler = HippoScheduler(self.redis_client)
        offer = Dict()
        cres = Dict()
        cres.name = 'cpus'
        cres.scalar.value = 1.0
        mres = Dict()
        mres.name = 'mem'
        mres.scalar.value = 1024
        offer.resources = [cres,mres]
        offer.id = 'foooffer'
        offer.agent_id.value = 'fooagent'
        offers = [offer]
        scheduler.resourceOffers(driver,offers)

    def test_getResource(self):
        scheduler = HippoScheduler(self.redis_client)
        d = Dict()
        d.name = 'foo'
        d.scalar.value = 1.0
        self.assertEqual(scheduler.getResource([d],'foo'),1.0)

    def test_statusUpdate(self):
        x = HippoTask(definition={'id':'foo'},redis_client=self.redis_client)
        scheduler = HippoScheduler(self.redis_client)
        update = Dict()
        update.task_id.value = x.mesos_id
        update.state = 'TASK_FAILED'
        scheduler.statusUpdate(None,update)
        x.load()
        self.assertEqual(x.definition['mesos_state'],'TASK_FAILED')


if __name__ == '__main__':
    unittest.main()