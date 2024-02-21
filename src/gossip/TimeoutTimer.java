package gossip;

import java.util.Date;

import javax.management.timer.Timer;

public class TimeoutTimer extends Timer {

	private Member owner; // timer owner
	private long sleepTime; // num of waiting seconds

	public TimeoutTimer(long sleepTime, Client client, Member member) {
		super();
		this.sleepTime = sleepTime;
		this.owner = member;
		addNotificationListener(client, null, null);
	}

	public void start() {
		this.reset();
		super.start();
	}

	public void reset() {
		removeAllNotifications();
		setWakeupTime(sleepTime);
	}

	private void setWakeupTime(long milliseconds) {
		addNotification("type", "message", owner, new Date(System.currentTimeMillis()+milliseconds));
	}
}

