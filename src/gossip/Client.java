package gossip;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;
import javax.management.NotificationListener;

public class Client implements NotificationListener {

	private ArrayList<Member> memberList;

	private ArrayList<Member> deadList;

	private int gossipTime; //in ms
	public int cleanupTime; //in ms

	private Random random;

	private DatagramSocket server;

	private String myAddress;

	private Member me;

	public Client() throws SocketException, InterruptedException, UnknownHostException {

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				System.out.println("Shutting down...");
			}
		}));

		memberList = new ArrayList<Member>();

		deadList = new ArrayList<Member>();

		gossipTime = 100; // 1 second TODO: make configurable

		cleanupTime = 10000; // 10 seconds TODO: make configurable

		random = new Random();

		int port = 0;

		String myIpAddress = InetAddress.getLocalHost().getHostAddress();
		this.myAddress = myIpAddress + ":" + port;

		ArrayList<String> hostsList = parseStartupMembers();

		for (String host : hostsList) {

			Member member = new Member(host, 0, this, cleanupTime);

			if(host.contains(myIpAddress)) {
				me = member;
				port = Integer.parseInt(host.split(":")[1]);
				this.myAddress = myIpAddress + ":" + port;
				System.out.println("I am " + me);
			}
			memberList.add(member);
		}

		System.out.println("Original Member List");
		System.out.println("---------------------");
		for (Member member : memberList) {
			System.out.println(member);
		}

		if(port != 0) {
			server = new DatagramSocket(port);
		}
		else {
			System.err.println("ERROR");
			System.exit(-1);
		}
	}

	private ArrayList<String> parseStartupMembers() {
		ArrayList<String> hostList = new ArrayList<String>();
		File hostFile = new File("config","start_ips");

		try (BufferedReader br = new BufferedReader(new FileReader(hostFile))) {
			String l;
			while((l = br.readLine()) != null) {
				hostList.add(l.trim());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return hostList;
	}

	private void sendMembershipList() {
		this.me.setHeartbeat(me.getHeartbeat() + 1);
		synchronized (this.memberList) {
			try {
				Member member = getRandomMember();
				if(member != null) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(baos);
					oos.writeObject(this.memberList);
					byte[] buf = baos.toByteArray();

					String address = member.getAddress();
					String host = address.split(":")[0];
					int port = Integer.parseInt(address.split(":")[1]);

					InetAddress dest;
					dest = InetAddress.getByName(host);

					System.out.println("Sending to " + dest);
					System.out.println("---------------------");
					for (Member m : memberList) {
						System.out.println(m);
					}
					System.out.println("---------------------");
					
					//simulate some packet loss ~30%
					int percentToSend = random.nextInt(100);
					if(percentToSend > 30) {
						DatagramSocket socket = new DatagramSocket();
						DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, dest, port);
						socket.send(datagramPacket);
						socket.close();
					}
				}

			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private Member getRandomMember() {
		Member member = null;

		if(this.memberList.size() > 1) {
			int tryAgain = 15;
			do {
				int rid = random.nextInt(this.memberList.size());
				member = this.memberList.get(rid);
				if(--tryAgain <= 0) {
					member = null;
					break;
				}
			} while(member.getAddress().equals(this.myAddress));
		}
		else {
			System.out.println("Alone!");
		}

		return member;
	}

	private class Sender implements Runnable {

		private AtomicBoolean running;

		public Sender() {
			this.running = new AtomicBoolean(true);
		}

		@Override
		public void run() {
			while(this.running.get()) {
				try {
					TimeUnit.MILLISECONDS.sleep(gossipTime);
					sendMembershipList();
				} catch (InterruptedException e) {
					e.printStackTrace();
					running.set(false);
				}
			}

			this.running = null;
		}

	}

	private class Receiver implements Runnable {

		private AtomicBoolean running;

		public Receiver() {
			running = new AtomicBoolean(true);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			while(running.get()) {
				try {

					byte[] buf = new byte[256];
					DatagramPacket p = new DatagramPacket(buf, buf.length);
					server.receive(p);

					ByteArrayInputStream bais = new ByteArrayInputStream(p.getData());
					ObjectInputStream ois = new ObjectInputStream(bais);

					Object readObject = ois.readObject();
					if(readObject instanceof ArrayList<?>) {
						ArrayList<Member> list = (ArrayList<Member>) readObject;

						System.out.println("Received member list:");
						for (Member member : list) {
							System.out.println(member);
						}
						// Merge our list with the one we just received
						mergeLists(list);
					}

				} catch (IOException e) {
					e.printStackTrace();
					running.set(false);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}

		private void mergeLists(ArrayList<Member> remoteList) {
			synchronized (Client.this.deadList) {
				synchronized (Client.this.memberList) {
					for (Member remoteMember : remoteList) {
						if(Client.this.memberList.contains(remoteMember)) {
							Member localMember = Client.this.memberList.get(Client.this.memberList.indexOf(remoteMember));
							if(remoteMember.getHeartbeat() > localMember.getHeartbeat()) {
								// update local list with latest heartbeat
								localMember.setHeartbeat(remoteMember.getHeartbeat());
								// and reset the timeout of that member
								localMember.resetBombTimer();
							}
						}
						else {
							// the local list does not contain the remote member

							// the remote member is either brand new, or a previously declared dead member
							// if its dead, check the heartbeat because it may have come back from the dead

							if(Client.this.deadList.contains(remoteMember)) {
								Member localDeadMember = Client.this.deadList.get(Client.this.deadList.indexOf(remoteMember));
								if(remoteMember.getHeartbeat() > localDeadMember.getHeartbeat()) {
									// it's baa-aack
									Client.this.deadList.remove(localDeadMember);
									Member newLocalMember = new Member(remoteMember.getAddress(), remoteMember.getHeartbeat(), Client.this, cleanupTime);
									Client.this.memberList.add(newLocalMember);
									newLocalMember.startBombTimer();
								} // else ignore
							}
							else {
								Member newLocalMember = new Member(remoteMember.getAddress(), remoteMember.getHeartbeat(), Client.this, cleanupTime);
								Client.this.memberList.add(newLocalMember);
								newLocalMember.startBombTimer();
							}
						}
					}
				}
			}
		}
	}

	private void start() throws InterruptedException {
		for (Member member : memberList) {
			if(member != me) {
				member.startBombTimer();
			}
		}

		ExecutorService executor = Executors.newCachedThreadPool();

		executor.execute(new Receiver());
		executor.execute(new Sender());

		while(true) {
			TimeUnit.SECONDS.sleep(10);
		}
	}

	public static void main(String[] args) throws InterruptedException, SocketException, UnknownHostException {

		Client client = new Client();
		client.start();
	}


	@Override
	public void handleNotification(Notification notification, Object handback) { // all dead timers will execute this

		Member deadMember = (Member) notification.getUserData();

		System.out.println("Dead member detected: " + deadMember);

		synchronized (this.memberList) {
			this.memberList.remove(deadMember);
		}

		synchronized (this.deadList) {
			this.deadList.add(deadMember);
		}

	}
}
