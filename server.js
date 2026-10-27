const http=require("http"),fs=require("fs"),path=require("path");
const rooms=new Map();
const send=(res,ev,d)=>{try{res.write("event: "+ev+"\ndata: "+JSON.stringify(d)+"\n\n")}catch(e){}};
const bc=r=>{const s=[...r.entries()].map(([peer,p])=>({peer,presence:p.presence}));r.forEach(p=>send(p.res,"peers",s))};
http.createServer((q,s)=>{
  const u=new URL(q.url,"http://x");
  if(u.pathname==="/events"){
    const code=u.searchParams.get("room")||"mesa",id=u.searchParams.get("id");
    if(!id){s.writeHead(400).end();return}
    s.writeHead(200,{"Content-Type":"text/event-stream","Cache-Control":"no-cache","X-Accel-Buffering":"no"});
    let r=rooms.get(code);if(!r)rooms.set(code,r=new Map());
    const old=r.get(id);
    r.set(id,{res:s,presence:old?old.presence:{}});
    send(s,"hello",{});bc(r);
    const ka=setInterval(()=>{try{s.write(":\n\n")}catch(e){}},20000);
    q.on("close",()=>{clearInterval(ka);setTimeout(()=>{const e=r.get(id);if(e&&e.res===s){r.delete(id);if(r.size)bc(r);else rooms.delete(code)}},8000)});
    return;
  }
  if(u.pathname==="/presence"&&q.method==="POST"){
    let b="";
    q.on("data",c=>{b+=c;if(b.length>5e5)q.destroy()});
    q.on("end",()=>{
      try{
        const{room,id,patch}=JSON.parse(b),r=rooms.get(room),p=r&&r.get(id);
        if(p){for(const k in patch){if(patch[k]===null)delete p.presence[k];else p.presence[k]=patch[k]}bc(r)}
        s.writeHead(204).end();
      }catch(e){s.writeHead(400).end()}
    });
    return;
  }
  fs.readFile(path.join(__dirname,"index.html"),(e,d)=>{
    if(e){s.writeHead(500).end("index.html não encontrado");return}
    s.writeHead(200,{"Content-Type":"text/html; charset=utf-8"}).end(d);
  });
}).listen(process.env.PORT||3000,()=>console.log("Jogo no ar na porta "+(process.env.PORT||3000)));
