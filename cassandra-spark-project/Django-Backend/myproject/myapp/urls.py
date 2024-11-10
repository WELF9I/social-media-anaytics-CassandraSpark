from django.urls import path
from .views import PostStatsView, PostAggregatesView, HashtagStatsView, RealTimeStatsView

app_name = 'analytics'

urlpatterns = [
    path('posts/stats/', PostStatsView.as_view(), name='post-stats'),
    path('posts/aggregates/', PostAggregatesView.as_view(), name='post-aggregates'),
    path('hashtags/stats/', HashtagStatsView.as_view(), name='hashtag-stats'),
    path('realtime/', RealTimeStatsView.as_view(), name='realtime-stats'),
]