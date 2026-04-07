function test() {
	const ws = new WebSocket("ws://localhost:8080/tether");
	ws.onmessage = (event) => {
		console.log(event.data);
	};
	ws.onopen = () => {
		console.log("Connected to server");
        ws.send(JSON.stringify({
            type: "subscribe",
            channel: "messages",
        }));
	};
	ws.onclose = () => {
		console.log("Disconnected from server");
	};
}
test();