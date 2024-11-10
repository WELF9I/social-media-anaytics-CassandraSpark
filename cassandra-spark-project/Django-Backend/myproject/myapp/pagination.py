from rest_framework.pagination import CursorPagination

class TimestampPagination(CursorPagination):
    ordering = '-created_at'
    page_size = 50
