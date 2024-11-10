from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from datetime import datetime, timedelta
import random
import time
import uuid
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SocialMediaDataGenerator:
    def __init__(self):
        profile = ExecutionProfile(load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='dc1'))
        self.cluster = Cluster(contact_points=['localhost'], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        self.session = self.cluster.connect('social_media_analytics')

        # Prepare statements for better performance
        self.post_stats_stmt = self.session.prepare("""
            INSERT INTO post_stats (post_id, timestamp, likes, shares, comments, hashtags)
            VALUES (?, ?, ?, ?, ?, ?)
        """)

        self.post_counters_stmt = self.session.prepare("""
            UPDATE post_counters 
            SET likes = likes + ?, shares = shares + ?, comments = comments + ?
            WHERE post_id = ? AND day_bucket = ?
        """)

        self.hashtag_stats_stmt = self.session.prepare("""
            UPDATE hashtag_stats
            SET usage_count = usage_count + 1
            WHERE day_bucket = ? AND hashtag = ?
        """)

        self.hashtags_pool = [
            '#tech', '#AI', '#programming', '#python', '#data',
            '#cloud', '#coding', '#developer', '#software', '#innovation'
        ]

    def get_day_bucket(self, timestamp):
        """Returns the start of the day for the given timestamp."""
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)

    def insert_stats(self, data):
        """Inserts statistics data into Cassandra tables."""
        try:
            # Get the day bucket for time-based partitioning
            day_bucket = self.get_day_bucket(data['timestamp'])

            # Insert into post_stats table
            self.session.execute(
                self.post_stats_stmt,
                (
                    data['post_id'],
                    data['timestamp'],
                    data['likes'],
                    data['shares'],
                    data['comments'],
                    data['hashtags']
                )
            )

            # Update post_counters table
            self.session.execute(
                self.post_counters_stmt,
                (
                    data['likes'],
                    data['shares'],
                    data['comments'],
                    data['post_id'],
                    day_bucket
                )
            )

            # Update hashtag_stats table for each hashtag
            for hashtag in data['hashtags']:
                self.session.execute(
                    self.hashtag_stats_stmt,
                    (day_bucket, hashtag)
                )

            logger.info(f"Data inserted successfully for post {data['post_id']}")

        except Exception as e:
            logger.error(f"Error during insertion: {e}")
            raise

    def generate_post_data(self):
        """Generates random post data with consistent types."""
        return {
            'post_id': uuid.uuid4(),  # Changed to UUID type
            'timestamp': datetime.now(),
            'likes': random.randint(0, 100),
            'shares': random.randint(0, 30),
            'comments': random.randint(0, 50),
            'hashtags': set(random.sample(self.hashtags_pool, random.randint(1, 4)))  # Using set instead of list
        }

    def run(self, duration_minutes=10):
        """Runs the data generator for the specified duration."""
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)

        try:
            while time.time() < end_time:
                data = self.generate_post_data()
                self.insert_stats(data)
                time.sleep(1)  # Wait 1 second between insertions

            logger.info("Data generation completed successfully")

        except Exception as e:
            logger.error(f"Error during data generation: {e}")
            raise

        finally:
            self.cleanup()

    def cleanup(self):
        """Closes Cassandra connections properly."""
        if not self.cluster.is_shutdown:
            self.cluster.shutdown()


if __name__ == "__main__":
    generator = SocialMediaDataGenerator()
    try:
        generator.run()
    except Exception as e:
        logger.error(f"Application error: {e}")