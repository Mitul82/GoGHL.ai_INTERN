import { useEffect, useRef, useState } from 'react';

interface WorkspaceRole {
  id: string;
  role: 'admin' | 'agent';
}

interface MeResponse {
  sub: string;
  workspaces: WorkspaceRole[];
}

interface ConversationItem {
  workspaceId: string;
  contact: string;
  assignee: string | null;
  status: 'open' | 'closed';
  lastActivityAt: string;
}

interface InboundItem {
  messageId: string;
  text: string;
  sortOrder: number;
}

interface OutboundItem {
  clientRef: string;
  text: string;
  status: 'queued' | 'sent' | 'delivered' | 'failed';
}

export function App() {
  const [me, setMe] = useState<MeResponse | null>(null);
  const [loadingMe, setLoadingMe] = useState(true);
  const [workspaceId, setWorkspaceId] = useState('');
  const [conversations, setConversations] = useState<ConversationItem[]>([]);
  const [selectedContact, setSelectedContact] = useState('');
  const [inbound, setInbound] = useState<InboundItem[]>([]);
  const [outbound, setOutbound] = useState<OutboundItem[]>([]);
  const [replyText, setReplyText] = useState('');
  const [notice, setNotice] = useState<string | null>(null);

  const lastSeqRef = useRef<number>(0);

  useEffect(() => {
    fetch('http://localhost:4000/api/me', { credentials: 'include' })
      .then(async (res) => (res.ok ? ((await res.json()) as MeResponse) : null))
      .then((user) => {
        setMe(user);
        if (user?.workspaces?.length) {
          setWorkspaceId(user.workspaces[0].id);
        }
      })
      .finally(() => setLoadingMe(false));
  }, []);

  async function refreshConversations(ws: string) {
    const res = await fetch(`http://localhost:4000/api/conversations?workspace=${encodeURIComponent(ws)}`, {
      credentials: 'include',
    });
    if (!res.ok) {
      setNotice(`Unable to load conversations (${res.status})`);
      return;
    }
    const data = (await res.json()) as { conversations: ConversationItem[] };
    setConversations(data.conversations || []);
  }

  async function refreshMessages(ws: string, contact: string) {
    const res = await fetch(
      `http://localhost:4000/api/conversations/${encodeURIComponent(contact)}/messages?workspace=${encodeURIComponent(ws)}`,
      { credentials: 'include' }
    );
    if (!res.ok) return;
    const data = (await res.json()) as { inbound: InboundItem[]; outbound: OutboundItem[] };
    setInbound(data.inbound || []);
    setOutbound(data.outbound || []);
  }

  useEffect(() => {
    if (!workspaceId) return;
    lastSeqRef.current = 0;
    setNotice(null);
    refreshConversations(workspaceId);
    if (selectedContact) {
      refreshMessages(workspaceId, selectedContact);
    }

    let closedByCleanup = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let socket: WebSocket | null = null;

    const connect = () => {
      if (closedByCleanup) return;
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const url = `${proto}//${window.location.host}/ws?workspace=${encodeURIComponent(
        workspaceId
      )}&after=${lastSeqRef.current}`;

      socket = new WebSocket(url);
      socket.onmessage = (event) => {
        try {
          const frame = JSON.parse(event.data) as {
            seq?: number;
            type?: string;
            contact?: string;
            client_ref?: string;
            status?: OutboundItem['status'];
            assignee?: string | null;
          };
          if (typeof frame.seq === 'number' && frame.seq > lastSeqRef.current) {
            lastSeqRef.current = frame.seq;
          }
          if (frame.type === 'message.inbound') {
            refreshConversations(workspaceId);
            if (frame.contact === selectedContact) {
              refreshMessages(workspaceId, selectedContact);
            }
          } else if (frame.type === 'message.status' && frame.client_ref && frame.status) {
            setOutbound((prev) =>
              prev.map((m) =>
                m.clientRef === frame.client_ref ? { ...m, status: frame.status! } : m
              )
            );
          } else if (frame.type === 'conversation.assigned' && frame.contact) {
            setConversations((prev) =>
              prev.map((c) =>
                c.contact === frame.contact ? { ...c, assignee: frame.assignee ?? null } : c
              )
            );
          }
        } catch {
          // ignore malformed frames
        }
      };

      socket.onclose = () => {
        if (!closedByCleanup) {
          reconnectTimer = setTimeout(connect, 1000);
        }
      };
    };

    connect();

    return () => {
      closedByCleanup = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [workspaceId, selectedContact]);

  async function onClaim(contact: string) {
    setNotice(null);
    const res = await fetch(`http://localhost:4000/api/conversations/${encodeURIComponent(contact)}/claim`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ workspace: workspaceId }),
    });
    if (res.status === 409) {
      const data = (await res.json().catch(() => ({}))) as { assignee?: string | null };
      setNotice(`Already claimed by ${data.assignee || 'another agent'}`);
    }
    await refreshConversations(workspaceId);
  }

  async function onToggleStatus(contact: string, current: 'open' | 'closed') {
    const next = current === 'open' ? 'closed' : 'open';
    await fetch(`http://localhost:4000/api/conversations/${encodeURIComponent(contact)}/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ workspace: workspaceId, status: next }),
    });
    await refreshConversations(workspaceId);
  }

  async function onSend(e: React.FormEvent) {
    e.preventDefault();
    if (!selectedContact || !replyText.trim()) return;
    const text = replyText.trim();
    const clientRef = `ref_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    setReplyText('');

    setOutbound((prev) => [...prev, { clientRef, text, status: 'queued' }]);

    await fetch('http://localhost:4000/api/replies', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({
        workspace: workspaceId,
        contact: selectedContact,
        text,
        client_ref: clientRef,
      }),
    });
  }

  if (loadingMe) {
    return <main style={{ padding: 24, fontFamily: 'system-ui' }}>Loading…</main>;
  }

  if (!me) {
    return (
      <main style={{ padding: 32, fontFamily: 'system-ui', maxWidth: 520, margin: '64px auto' }}>
        <h1>Switchboard</h1>
        <p>Sign in with the Mock IdP to open your workspace inbox.</p>
        <a href="/auth/login">Sign in with IdP</a>
      </main>
    );
  }

  const activeConversation = conversations.find((c) => c.contact === selectedContact) || null;

  return (
    <div style={{ display: 'flex', height: '100vh', fontFamily: 'system-ui' }}>
      <aside style={{ width: 320, borderRight: '1px solid #e5e7eb', padding: 16, overflowY: 'auto' }}>
        <div style={{ marginBottom: 14 }}>
          <div style={{ fontWeight: 600 }}>{me.sub}</div>
          <select
            value={workspaceId}
            onChange={(e) => {
              setSelectedContact('');
              setWorkspaceId(e.target.value);
            }}
            style={{ marginTop: 8, padding: 6 }}
          >
            {me.workspaces.map((w) => (
              <option key={w.id} value={w.id}>
                {w.id} ({w.role})
              </option>
            ))}
          </select>
        </div>

        {notice && (
          <div style={{ color: '#b91c1c', fontSize: 13, marginBottom: 10 }}>{notice}</div>
        )}

        {conversations.map((conv) => (
          <div
            key={conv.contact}
            onClick={() => setSelectedContact(conv.contact)}
            style={{
              padding: 10,
              borderRadius: 6,
              marginBottom: 6,
              cursor: 'pointer',
              background: conv.contact === selectedContact ? '#eef2ff' : '#f9fafb',
            }}
          >
            <div style={{ fontWeight: 600 }}>{conv.contact}</div>
            <div style={{ fontSize: 12, color: '#4b5563' }}>
              {conv.assignee ? `Claimed: ${conv.assignee}` : 'Unassigned'} · {conv.status}
            </div>
          </div>
        ))}
      </aside>

      <main style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: 16 }}>
        {!selectedContact ? (
          <div style={{ color: '#6b7280' }}>Select a conversation to view messages and reply.</div>
        ) : (
          <>
            <header
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                paddingBottom: 12,
                borderBottom: '1px solid #e5e7eb',
              }}
            >
              <div>
                <h2 style={{ margin: 0 }}>{selectedContact}</h2>
                <small style={{ color: '#4b5563' }}>
                  Assignee: {activeConversation?.assignee || 'Unassigned'} · Status:{' '}
                  {activeConversation?.status || 'open'}
                </small>
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button type="button" onClick={() => onClaim(selectedContact)}>
                  Claim
                </button>
                <button
                  type="button"
                  onClick={() =>
                    onToggleStatus(selectedContact, activeConversation?.status || 'open')
                  }
                >
                  {activeConversation?.status === 'closed' ? 'Reopen' : 'Close'}
                </button>
              </div>
            </header>

            <section
              style={{
                flex: 1,
                overflowY: 'auto',
                padding: '12px 0',
                display: 'flex',
                flexDirection: 'column',
                gap: 8,
              }}
            >
              {inbound.map((m) => (
                <div
                  key={m.messageId}
                  style={{
                    alignSelf: 'flex-start',
                    background: '#f3f4f6',
                    padding: '8px 12px',
                    borderRadius: 8,
                  }}
                >
                  <div>{m.text}</div>
                  <small style={{ color: '#6b7280' }}>Customer · {m.messageId}</small>
                </div>
              ))}
              {outbound.map((m) => (
                <div
                  key={m.clientRef}
                  style={{
                    alignSelf: 'flex-end',
                    background: '#dbeafe',
                    padding: '8px 12px',
                    borderRadius: 8,
                  }}
                >
                  <div>{m.text}</div>
                  <small style={{ color: '#1e40af' }}>Agent · {m.status}</small>
                </div>
              ))}
            </section>

            <form onSubmit={onSend} style={{ display: 'flex', gap: 8 }}>
              <input
                value={replyText}
                onChange={(e) => setReplyText(e.target.value)}
                placeholder="Write a reply…"
                style={{ flex: 1, padding: 8 }}
              />
              <button type="submit">Send</button>
            </form>
          </>
        )}
      </main>
    </div>
  );
}