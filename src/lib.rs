extern crate zmq;
extern crate log;
use log::{info, debug};

#[derive(Debug, PartialEq, Clone)]
pub enum RRMessageCommand{
    READY=0,
    OKAY,
    DISCONNECT,
    HEARTBEAT,
    REQ,
    REP,
    ERROR,
}
impl Default for RRMessageCommand{
    fn default() -> Self{Self::OKAY}
}

#[derive(Debug, PartialEq)]
pub enum MessageFormat{
    NONE=0,
    TEXT,
    JSON,
    MSGPACK,
}

impl Default for MessageFormat{
    fn default() -> Self{MessageFormat::NONE}
}

#[derive(Default, Debug)]
pub struct MsgMetadata{
    pub identity:Option<Vec<u8>>,
    // empty 
    pub version:String,
    pub uid:Vec<u8>,
    pub cmd:RRMessageCommand,
}

#[derive(Default, Debug)]
pub struct MsgContent{
    pub target:Option<String>,
    pub format:MessageFormat,
    pub content:Vec<u8>,
}

#[derive(Debug)]
pub enum RRMessage{
    READY(MsgMetadata, Option<String>),
    OKAY(MsgMetadata),
    DISCONNECT(MsgMetadata),
    HEARTBEAT(MsgMetadata),
    REQ(MsgMetadata, MsgContent),
    REP(MsgMetadata, MsgContent),
    ERROR(MsgMetadata, MsgContent),
}

pub struct RRMsgHandler;
impl RRMsgHandler{
    pub fn create_ready(version:String, msgid:Option<Vec<u8>>, target:Option<String>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::READY, None, version, msgid, target, None)
    }

    pub fn create_okay(id:Option<Vec<u8>>, version:String, msgid:Option<Vec<u8>>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::OKAY, id, version, msgid, None, None)
    }

    pub fn create_disconnect(version:String, msgid:Option<Vec<u8>>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::DISCONNECT, None, version, msgid, None,None)
    }

    pub fn create_heartbeat(version:String, msgid:Option<Vec<u8>>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::HEARTBEAT, None, version, msgid, None, None)
    }

    pub fn create_request(version:String, msgid:Option<Vec<u8>>, target:Option<String>, format:MessageFormat, content:Vec<u8>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::REQ, None, version, msgid, target, Some((format, content)))
    }

    pub fn create_reply(id:Option<Vec<u8>>, version:String, msgid:Option<Vec<u8>>, target:Option<String>, format:MessageFormat, content:Vec<u8>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::REP, id, version, msgid, target, Some((format, content)))
    }

    pub fn create_error(version:String, msgid:Option<Vec<u8>>, target:Option<String>, format:MessageFormat, content:Vec<u8>)->Option<RRMessage>{
        Self::build_rrmsg(RRMessageCommand::ERROR, None, version, msgid, target, Some((format, content)))
    }

    pub fn build_rrmsg(cmd:RRMessageCommand, id:Option<Vec<u8>>, version:String, msgid:Option<Vec<u8>>, target:Option<String>, contents:Option<(MessageFormat, Vec<u8>)>)->Option<RRMessage>{
        // metadata
        let mut meta = MsgMetadata{identity:id, version, cmd:cmd.clone(), ..Default::default()};
        if let Some(v) = msgid{ meta.uid = v; }

        match cmd{
            RRMessageCommand::READY=>{
                Some(RRMessage::READY(meta, target))
            },
            RRMessageCommand::OKAY=>{
                Some(RRMessage::OKAY(meta))
            },
            RRMessageCommand::DISCONNECT=>{
                Some(RRMessage::DISCONNECT(meta))
            },
            RRMessageCommand::HEARTBEAT=>{
                Some(RRMessage::HEARTBEAT(meta))
            },
            RRMessageCommand::REQ| RRMessageCommand::REP| RRMessageCommand::ERROR=>{
                let mut content = MsgContent{target, ..Default::default()};
                if let Some(cont) = contents{
                    content.format = cont.0;
                    content.content = cont.1;
                }
                if cmd == RRMessageCommand::REQ{
                    Some(RRMessage::REQ(meta, content))
                }else if cmd == RRMessageCommand::REP{
                    Some(RRMessage::REP(meta, content))
                } else{
                    Some(RRMessage::ERROR(meta, content))
                }
            },
        }
    }

    pub fn send_rrmeta_msg(socket:&zmq::Socket, meta: MsgMetadata, last:bool)->zmq::Result<()>{
        // RR ID
        if let Some(id) = meta.identity{
            socket.send(&id, zmq::SNDMORE)?;
        }
        // empty
        socket.send("", zmq::SNDMORE)?;

        // version
        socket.send(&meta.version, zmq::SNDMORE)?;

        // message ID
        socket.send(meta.uid, zmq::SNDMORE)?;

        // cmd
        let cmd = meta.cmd as u8;
        socket.send(vec!(cmd), if last{0}else{zmq::SNDMORE})?;
        Ok(())
    }

    pub fn send_rrcontent_msg(socket:&zmq::Socket, content:MsgContent)->zmq::Result<()>{
        // target ID
        if let Some(tid) = content.target{
            socket.send(&tid, zmq::SNDMORE)?;
        }else{
            socket.send("", zmq::SNDMORE)?;
        }

        // msg type
        let more = content.format != MessageFormat::NONE;
        let msgtype= content.format as u8;
        socket.send(vec!(msgtype), if more {zmq::SNDMORE} else{0})?;
        
        // contents
        if more{
            socket.send(content.content, 0)?;
        }
        Ok(())
    }


    pub fn send_rrmsg(socket:&zmq::Socket, msg:RRMessage)->zmq::Result<()>{
        match msg{
            RRMessage::READY(meta, selfid)=>{
                let has_selfid = selfid.is_some();
                Self::send_rrmeta_msg(socket, meta, !has_selfid)?;
                if has_selfid{
                    socket.send(&selfid.unwrap(), 0)?;
                }
            },
            RRMessage::OKAY(meta)=>{
                Self::send_rrmeta_msg(socket, meta, true)?;
            },
            RRMessage::DISCONNECT(meta)=>{
                Self::send_rrmeta_msg(socket, meta, true)?;
            },
            RRMessage::HEARTBEAT(meta)=>{
                Self::send_rrmeta_msg(socket, meta, true)?;
            },
            RRMessage::REQ(meta, content)=>{
                Self::send_rrmeta_msg(socket, meta, false)?;
                Self::send_rrcontent_msg(socket, content)?;
            },
            RRMessage::REP(meta, content)=>{
                Self::send_rrmeta_msg(socket, meta, false)?;
                Self::send_rrcontent_msg(socket, content)?;
            },
            RRMessage::ERROR(meta, content)=>{
                Self::send_rrmeta_msg(socket, meta, false)?;
                Self::send_rrcontent_msg(socket, content)?;
            },
        }
        Ok(())
    }

    pub fn parse_rrmsg(socket:&zmq::Socket, byte:&mut Vec<u8>) -> zmq::Result<Option<RRMessage>>{
        debug!("rrmessage parsing started");
        let mut meta = MsgMetadata{..Default::default()};

        // 1. identity or empty frame
        let size = socket.recv_into(byte, 0)?;
        // check from id or start of message
        debug!("check identity or empty frame via size {}", size );
        if size != 0{
            meta.identity = Some(byte[..size].to_vec());
            debug!("identity : {:?}", meta.identity);

            // pass first frame of reqrep msg
            let _ = socket.recv_into(byte, 0)?;
        }

        // 2. protocol version
        if socket.get_rcvmore()?{
            let size = socket.recv_into(byte, 0)?;
            if size != 0{ meta.version = std::str::from_utf8(&byte[..size]).unwrap().to_string();}
            debug!(" protocol version {:?}", meta.version);
        }else{
            return Ok(None);
        }

        // 3. RRMessage uid
        if socket.get_rcvmore()?{
            let size = socket.recv_into(byte, 0)?;
            if size != 0{ meta.uid = byte[..size].to_vec(); }
            debug!(" message uid {:?}", meta.uid);
        }else{
            return Ok(None);
        }


        let mut cmd:u8 = u8::MAX;
        // 4. Command
        if socket.get_rcvmore()?{
            let size = socket.recv_into(byte, 0)?;
            if size == 1{ 
                cmd = byte[0]; 
            }
            debug!(" cmd {:?} if {} then failed", cmd, u8::MAX);
            if size != 1{return Ok(None);}
        }else{
            return Ok(None);
        }

        match cmd{
            1 =>{meta.cmd = RRMessageCommand::OKAY; return Ok(Some(RRMessage::OKAY(meta)))},
            2 =>{meta.cmd = RRMessageCommand::DISCONNECT; return Ok(Some(RRMessage::DISCONNECT(meta)))},
            3 =>{meta.cmd = RRMessageCommand::HEARTBEAT; return Ok(Some(RRMessage::HEARTBEAT(meta)))},
            _=>{},
        }
        
        let mut content = MsgContent{..Default::default()};
        // 5. target name
        if socket.get_rcvmore()?{
            let size = socket.recv_into(byte, 0)?;
            if size != 0{
                content.target = Some(std::str::from_utf8(&byte[..size]).unwrap().to_string());
            }
            debug!(" target id {:?}", content.target);
        }else{
            return Ok(None);
        }

        // ... if READY message
        if cmd == 0 {
            meta.cmd = RRMessageCommand::READY;
            return Ok(Some(RRMessage::READY(meta, content.target)));
        }

        // 6. RRMessage Type
        if socket.get_rcvmore()?{
            let size = socket.recv_into(byte, 0)?;
            if size == 1{
                match byte[0]{
                    1=>{content.format = MessageFormat::TEXT},
                    2=>{content.format = MessageFormat::JSON},
                    3=>{content.format = MessageFormat::MSGPACK},
                    _=>{ }
                }
                debug!(" target id {:?}", content.format);
            }else {
                return Ok(None);
            }
        }else{
            return Ok(None);
        }

        // 7. BODY
        if socket.get_rcvmore()?{
            let size = socket.recv_into(byte, 0)?;
            if size != 0{
                content.content = byte[..size].to_vec();
            }
                debug!(" contents {:?}", content.content);
        }

        while socket.get_rcvmore()?{
            let _ = socket.recv_into(byte, 0)?;
        }

        match cmd{
            4=>{meta.cmd = RRMessageCommand::REQ;Ok(Some(RRMessage::REQ(meta, content)))},
            5=>{meta.cmd = RRMessageCommand::REP;Ok(Some(RRMessage::REP(meta, content)))},
            6=>{meta.cmd = RRMessageCommand::ERROR;Ok(Some(RRMessage::ERROR(meta, content)))},
            _=>{Ok(None)},
        }
    }
}

pub trait ZMQMessage{
    fn send_rrmsg(&self, msg:RRMessage) -> zmq::Result<()>;
    fn receive_rrmsg(&self) -> zmq::Result<Option<RRMessage>>;
}

impl ZMQMessage for zmq::Socket{
    fn send_rrmsg(&self, msg:RRMessage) -> zmq::Result<()>{
        RRMsgHandler::send_rrmsg(&self, msg)
    }

    fn receive_rrmsg(&self) -> zmq::Result<Option<RRMessage>>{
        let mut msg_bytes = vec![0;10_000_000];
        RRMsgHandler::parse_rrmsg(self, &mut msg_bytes)
    }
}

