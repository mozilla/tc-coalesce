import taskclustercoalesce.web as web
import unittest
import json
from mockredis import mock_redis_client
from mock import patch


class WebTestBase(unittest.TestCase):

    def setUp(self):
        web.app.config['TESTING'] = True
        web.app.prefix = self.prefix = 'testing.prefix.'
        web.app.redis = mock_redis_client()
        self.app = web.app.test_client()

    def tearDown(self):
        pass

    def ordered(self, obj):
        ''' Recursively sort object '''
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k, v in obj.items())
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj


class WebTestCase(WebTestBase):

    def test_root_static(self):
        rv = self.app.get('/')
        actual = json.loads(rv.data)
        expected = {'versions': ['v1']}
        self.assertEqual(self.ordered(actual), self.ordered(expected))
        self.assertEqual(rv.status_code, 200)

    @patch('time.time')
    @patch.object(web, 'starttime', 1.0)
    def test_ping_time(self, m_time):
        m_time.return_value = 2.0
        rv = self.app.get('/v1/ping')
        actual = json.loads(rv.data)
        expected = {'alive': True, 'uptime': 1.0}
        self.assertEqual(self.ordered(actual), self.ordered(expected))
        self.assertEqual(rv.status_code, 200)

    def test_coalesce_key_list_empty(self):
        rv = self.app.get('/v1/list')
        actual = json.loads(rv.data)
        expected = {self.prefix: []}
        self.assertEqual(self.ordered(actual), self.ordered(expected))
        self.assertEqual(rv.status_code, 200)

    def test_coalesce_key_list_single(self):
        web.app.redis.sadd(self.prefix + "list_keys", "single_key")
        with web.app.test_client() as c:
            rv = c.get('/v1/list')
            actual = json.loads(rv.data)
            expected = {self.prefix: ["single_key"]}
            self.assertEqual(self.ordered(actual), self.ordered(expected))
            self.assertEqual(rv.status_code, 200)

    def test_coalesce_key_list_multi(self):
        web.app.redis.sadd(self.prefix + 'list_keys',
                           'key_1', 'key_2', 'key_3')
        with web.app.test_client() as c:
            rv = c.get('/v1/list')
            actual = json.loads(rv.data)
            expected = {self.prefix: ['key_1', 'key_2', 'key_3']}
            self.assertEqual(self.ordered(actual), self.ordered(expected))
            self.assertEqual(rv.status_code, 200)

    def test_coalesce_task_list_empty(self):
        rv = self.app.get('/v1/list/sample.key.1')
        actual = json.loads(rv.data)
        expected = {'supersedes': []}
        self.assertEqual(self.ordered(actual), self.ordered(expected))
        self.assertEqual(rv.status_code, 200)

    def test_coalesce_task_list_single(self):
        web.app.redis.lpush(self.prefix + 'lists.' + 'sample.key.1', 'taskId1')
        with web.app.test_client() as c:
            rv = c.get('/v1/list/sample.key.1')
            actual = json.loads(rv.data)
            expected = {'supersedes': ['taskId1']}
            self.assertEqual(self.ordered(actual), self.ordered(expected))
            self.assertEqual(rv.status_code, 200)

    def test_coalesce_task_list_multi(self):
        web.app.redis.lpush(self.prefix + 'lists.' + 'sample.key.1',
                            'taskId1', 'taskId2', 'taskId3')
        with web.app.test_client() as c:
            rv = c.get('/v1/list/sample.key.1')
            actual = json.loads(rv.data)
            expected = {'supersedes': ['taskId1', 'taskId2', 'taskId3']}
            self.assertEqual(self.ordered(actual), self.ordered(expected))
            self.assertEqual(rv.status_code, 200)

    def test_stats_multi(self):
        stats = {'pending_count': '8',
                 'coalesced_lists': '7',
                 'unknown_tasks': '3',
                 'premature': '4',
                 'total_msgs_handled': '1'
                 }
        web.app.redis.hmset(self.prefix + "stats", stats)
        with web.app.test_client() as c:
            rv = c.get('/v1/stats')
            actual = json.loads(rv.data)
            expected = stats
            print(actual)
            print(expected)
            self.assertEqual(self.ordered(actual), self.ordered(expected))
            self.assertEqual(rv.status_code, 200)

if __name__ == '__main__':
    unittest.main()
