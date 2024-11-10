from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from datetime import datetime

class PostStats(Model):
  __table_name__ = 'post_stats'
  post_id = columns.UUID(primary_key=True, partition_key=True)
  # Rename timestamp to a unique name
  created_at = columns.DateTime(primary_key=True, clustering_order="DESC")
  likes = columns.Integer()
  shares = columns.Integer()
  comments = columns.Integer()
  hashtags = columns.Set(columns.Text())

class PostCounters(Model):
    __table_name__ = 'post_counters'
    post_id = columns.UUID(primary_key=True, partition_key=True)
    day_bucket = columns.DateTime(primary_key=True, clustering_order="DESC")
    likes = columns.Counter()
    shares = columns.Counter()
    comments = columns.Counter()

class HashtagStats(Model):
    __table_name__ = 'hashtag_stats'
    day_bucket = columns.DateTime(primary_key=True, partition_key=True)  # Changed order to match table structure
    hashtag = columns.Text(primary_key=True, clustering_order="ASC")
    usage_count = columns.Counter()

class PostAggregates(Model):
    __table_name__ = 'post_aggregates'
    window_start = columns.DateTime(primary_key=True, partition_key=True)
    post_id = columns.UUID(primary_key=True, clustering_order="ASC")
    total_likes = columns.BigInt()
    total_shares = columns.BigInt()
    total_comments = columns.BigInt()

class CassandraConnection:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
            from cassandra.policies import DCAwareRoundRobinPolicy
            from cassandra.auth import PlainTextAuthProvider
            from django.conf import settings
            
            # Ajout d'un profil d'exécution avec politique de load balancing
            profile = ExecutionProfile(
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1')
            )
            
            auth_provider = PlainTextAuthProvider(
                username=settings.CASSANDRA_AUTH_USER,
                password=settings.CASSANDRA_AUTH_PASSWORD
            )
            
            cluster = Cluster(
                settings.CASSANDRA_HOSTS,
                auth_provider=auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile}
            )
            session = cluster.connect(settings.CASSANDRA_KEYSPACE)
            cls._instance = session
            
        return cls._instance
