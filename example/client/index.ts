import { TetherClient } from "tether-ts";
import { createInterface } from "node:readline/promises";

const client = new TetherClient();
client.connect("ws://localhost:8080/tether");

client.subscribe("messages", { room: "1" }, (message) => {
	console.log("Received message", message);
});

const rl = createInterface({
  input: process.stdin,
  output: process.stdout,
});

while (true) {
  const message = await rl.question("Enter a message");
  if (message) {
    client.sendMutation("createMessage", { room: "1", message: message });
  }
}