from rest_framework import serializers
from datetime import datetime

class PostStatsSerializer(serializers.Serializer):
    post_id = serializers.UUIDField()
    timestamp = serializers.DateTimeField()
    likes = serializers.IntegerField(min_value=0)
    shares = serializers.IntegerField(min_value=0)
    comments = serializers.IntegerField(min_value=0)
    hashtags = serializers.ListField(child=serializers.CharField(), required=False)

    def to_representation(self, instance):
        # Handle both dict and model instance cases
        if isinstance(instance, dict):
            data = instance
        else:
            data = {
                'post_id': instance.post_id,
                'timestamp': instance.timestamp,
                'likes': instance.likes,
                'shares': instance.shares,
                'comments': instance.comments,
                'hashtags': instance.hashtags
            }
        
        # Convert set to list for hashtags if needed
        if isinstance(data['hashtags'], set):
            data['hashtags'] = list(data['hashtags'])
        return data

class PostAggregatesSerializer(serializers.Serializer):
    window_start = serializers.DateTimeField()
    post_id = serializers.UUIDField()
    total_likes = serializers.IntegerField(min_value=0)
    total_shares = serializers.IntegerField(min_value=0)
    total_comments = serializers.IntegerField(min_value=0)

    def to_representation(self, instance):
        # Handle both dict and model instance cases
        if isinstance(instance, dict):
            data = instance
        else:
            data = {
                'window_start': instance.window_start,
                'post_id': instance.post_id,
                'total_likes': instance.total_likes,
                'total_shares': instance.total_shares,
                'total_comments': instance.total_comments
            }
        
        # Ensure bigint values are properly serialized
        for field in ['total_likes', 'total_shares', 'total_comments']:
            if data[field] is not None:
                data[field] = int(data[field])
        return data

class HashtagStatsSerializer(serializers.Serializer):
    day_bucket = serializers.DateTimeField()
    hashtag = serializers.CharField()
    usage_count = serializers.IntegerField(min_value=0)

    def to_representation(self, instance):
        # Handle both dict and model instance cases
        if isinstance(instance, dict):
            data = instance
        else:
            data = {
                'day_bucket': instance.day_bucket,
                'hashtag': instance.hashtag,
                'usage_count': instance.usage_count
            }

        # Handle the day_bucket datetime conversion
        if isinstance(data['day_bucket'], str):
            try:
                # If it's already a string, parse it to datetime
                dt = datetime.strptime(data['day_bucket'], '%Y-%m-%d %H:%M:%S')
                data['day_bucket'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                # If parsing fails, return as is
                pass
        elif isinstance(data['day_bucket'], datetime):
            # If it's a datetime object, format it
            data['day_bucket'] = data['day_bucket'].strftime('%Y-%m-%d %H:%M:%S')
        
        return data