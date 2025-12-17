package notification

import (
    "sync"
)

type NotificationService struct {
    mu          sync.Mutex
    notifications []string
}

func NewNotificationService() *NotificationService {
    return &NotificationService{
        notifications: make([]string, 0),
    }
}

func (ns *NotificationService) AddNotification(notification string) {
    ns.mu.Lock()
    defer ns.mu.Unlock()
    ns.notifications = append(ns.notifications, notification)
}

func (ns *NotificationService) GetNotifications() []string {
    ns.mu.Lock()
    defer ns.mu.Unlock()
    return ns.notifications
}

func (ns *NotificationService) ClearNotifications() {
    ns.mu.Lock()
    defer ns.mu.Unlock()
    ns.notifications = []string{}
}