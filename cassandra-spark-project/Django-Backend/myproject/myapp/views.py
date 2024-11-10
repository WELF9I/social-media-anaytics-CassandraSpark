from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from datetime import datetime, timedelta
from .models import CassandraConnection
from .serializers import PostStatsSerializer, HashtagStatsSerializer, PostAggregatesSerializer
import logging

logger = logging.getLogger(__name__)

class PostStatsView(APIView):
    def get(self, request):
        try:
            session = CassandraConnection.get_instance()
            
            # Get recent statistics
            time_threshold = datetime.now() - timedelta(minutes=10)
            
            query_stats = """
                SELECT post_id, timestamp, likes, shares, comments, hashtags 
                FROM post_stats 
                WHERE timestamp > %s 
                ALLOW FILTERING
            """
            recent_stats = session.execute(query_stats, [time_threshold])
            
            # Get daily counters
            today_bucket = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            query_counters = """
                SELECT post_id, likes, shares, comments
                FROM post_counters
                WHERE day_bucket = %s
                ALLOW FILTERING
            """
            daily_counters = session.execute(query_counters, [today_bucket])
            
            # Combine results
            stats_data = []
            counter_dict = {c.post_id: c for c in daily_counters}
            
            for stat in recent_stats:
                counter_data = counter_dict.get(stat.post_id)
                
                combined_stat = {
                    'post_id': stat.post_id,
                    'timestamp': stat.timestamp,
                    'likes': stat.likes + (counter_data.likes if counter_data else 0),
                    'shares': stat.shares + (counter_data.shares if counter_data else 0),
                    'comments': stat.comments + (counter_data.comments if counter_data else 0),
                    'hashtags': list(stat.hashtags) if stat.hashtags else []
                }
                stats_data.append(combined_stat)
            
            serializer = PostStatsSerializer(stats_data, many=True)
            return Response(serializer.data)
            
        except Exception as e:
            logger.error(f"Error in PostStatsView: {str(e)}")
            return Response(
                {"error": "Failed to retrieve post statistics"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class HashtagStatsView(APIView):
    def get(self, request):
        try:
            session = CassandraConnection.get_instance()
            
            today_bucket = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            query = """
                SELECT hashtag, usage_count
                FROM hashtag_stats
                WHERE day_bucket = %s
                LIMIT 20
                ALLOW FILTERING
            """
            results = session.execute(query, [today_bucket])
            
            hashtag_data = [{
                'hashtag': row.hashtag,
                'usage_count': row.usage_count,
                'day_bucket': today_bucket
            } for row in results]
            
            serializer = HashtagStatsSerializer(hashtag_data, many=True)
            return Response(serializer.data)
            
        except Exception as e:
            logger.error(f"Error in HashtagStatsView: {str(e)}")
            return Response(
                {"error": "Failed to retrieve hashtag statistics"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class PostAggregatesView(APIView):
    def get(self, request):
        try:
            session = CassandraConnection.get_instance()
            
            time_threshold = datetime.now() - timedelta(hours=1)
            query = """
                SELECT * FROM post_aggregates 
                WHERE window_start > %s 
                LIMIT 100
                ALLOW FILTERING
            """
            results = session.execute(query, [time_threshold])
            
            aggregates_data = [{
                'window_start': row.window_start,
                'post_id': row.post_id,
                'total_likes': row.total_likes,
                'total_shares': row.total_shares,
                'total_comments': row.total_comments
            } for row in results]
            
            serializer = PostAggregatesSerializer(aggregates_data, many=True)
            return Response(serializer.data)
            
        except Exception as e:
            logger.error(f"Error in PostAggregatesView: {str(e)}")
            return Response(
                {"error": "Failed to retrieve post aggregates"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

class RealTimeStatsView(APIView):
    def get(self, request):
        try:
            session = CassandraConnection.get_instance()
            
            time_threshold = datetime.now() - timedelta(minutes=1)
            query = """
                SELECT * FROM post_stats 
                WHERE timestamp > %s 
                LIMIT 50
                ALLOW FILTERING
            """
            results = session.execute(query, [time_threshold])
            
            stats_data = [{
                'post_id': row.post_id,
                'timestamp': row.timestamp,
                'likes': row.likes,
                'shares': row.shares,
                'comments': row.comments,
                'hashtags': list(row.hashtags) if row.hashtags else []
            } for row in results]
            
            serializer = PostStatsSerializer(stats_data, many=True)
            return Response(serializer.data)
            
        except Exception as e:
            logger.error(f"Error in RealTimeStatsView: {str(e)}")
            return Response(
                {"error": "Failed to retrieve real-time statistics"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )