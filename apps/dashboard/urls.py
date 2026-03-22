# dashboard/urls.py
from django.urls import path
from django.contrib.auth.decorators import login_required
from . import views

app_name = 'dashboard'

urlpatterns = [
    path('', login_required(views.IndexView.as_view()), name='index'),
    path('topics/', login_required(views.TopicListView.as_view()), name='topic_list'),
    path('topics/<int:pk>/', login_required(views.TopicDetailView.as_view()), name='topic_detail'),
]