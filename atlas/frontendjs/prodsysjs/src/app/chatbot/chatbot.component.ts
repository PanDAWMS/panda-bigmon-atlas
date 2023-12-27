import {Component, OnInit} from '@angular/core';
import {webSocket, WebSocketSubject} from 'rxjs/webSocket';
import {NextObserver, Subscription} from "rxjs";


export interface ChatBotMessage {
  text: string;
  user: string;
  reply?: boolean;
}

@Component({
  selector: 'app-chatbot',
  templateUrl: './chatbot.component.html',
  styleUrls: ['./chatbot.component.css']
})
export class ChatbotComponent implements OnInit{

  public ws$: WebSocketSubject<ChatBotMessage> = webSocket({url: 'ws://ws/echo/',
    serializer: msg => JSON.stringify(msg), protocol: 'chat', openObserver: {
    next: (val: any) => {
      this.connected = true;
    }
   }
  });
  public receivedMessages: ChatBotMessage[] = [];
  public connected =  false;
  public webSocketConnection$: Subscription;

  ngOnInit() {

  }

  connect(): void {
    // Subscribe to the websocket and store the subscription so we can unsubscribe later if we want. Set the connected flag to true

    this.connected = true;

    this.webSocketConnection$ = this.ws$.subscribe(
      msg =>  {
        console.log('message received: ' + msg.text);
        this.receivedMessages.push(msg);
      },
      err => {
        console.log(err);
        this.receivedMessages.push({text: err?.error,  user: '', reply: false});

        this.connected = false;
      },
      // Called when connection is closed (for whatever reason)
      // Add message about connection closed
      () => {
        this.receivedMessages.push({text: 'Connection closed', user: '', reply: false});
        this.connected = false;
      }
    );

  }
  sendMessage(event): void {
    const newMessage: ChatBotMessage = {
      text: event,
      user: '',
      reply: true
    };
    this.ws$.next(newMessage);
    this.receivedMessages.push(newMessage);
  }
}
