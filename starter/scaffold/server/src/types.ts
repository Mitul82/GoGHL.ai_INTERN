export interface WorkspaceRole {
    id: string;
    role: 'admin' | 'agent';
  }
  
  export interface SessionPayload {
    sub: string;
    workspaces: WorkspaceRole[];
  }
  
  export interface ConversationDoc {
    workspaceId: string;
    contact: string;
    assignee: string | null;
    status: 'open' | 'closed';
    lastActivityAt: Date;
  }
  
  export interface InboundMessageDoc {
    workspaceId: string;
    contact: string;
    messageId: string;
    text: string;
    occurredAt: Date;
    sortOrder: number;
  }
  
  export interface OutboundMessageDoc {
    workspaceId: string;
    contact: string;
    clientRef: string;
    pigeonMessageId: string | null;
    text: string;
    status: 'queued' | 'sent' | 'delivered' | 'failed';
    statusRank: number;
    attempts: number;
    nextAttemptAt: Date;
    lockedBy: string | null;
    lockExpiresAt: Date | null;
    createdAt: Date;
  }
  
  export interface EventDoc {
    workspaceId: string;
    seq: number;
    type: 'message.inbound' | 'message.status' | 'conversation.assigned';
    payload: Record<string, unknown>;
  }