import { TetherClient } from "tether-ts";

const client = new TetherClient();
client.connect("ws://localhost:8080/tether");

client.subscribe("messages", (message) => {
	console.log("Received message", message);
});