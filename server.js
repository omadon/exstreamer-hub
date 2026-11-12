const express=require('express'),http=require('http'),path=require('path'),fs=require('fs'),dgram=require('dgram');
const {spawn}=require('child_process'),multer=require('multer'),{WebSocketServer}=require('ws'),snmp=require('net-snmp');
const DATA=process.env.DATA_DIR||'./data',UP=path.join(DATA,'uploads'),DB=path.join(DATA,'db.json');
fs.mkdirSync(UP,{recursive:true});
let db={devices:[],groups:[],playlists:[]};try{db=JSON.parse(fs.readFileSync(DB))}catch{}
const save=()=>fs.writeFileSync(DB,JSON.stringify(db,null,1)),uid=()=>Math.random().toString(36).slice(2,9);
const E=(c,m)=>Object.assign(new Error(m),{code:c});
const stats={},jobs=new Map(),busy=new Map(),log=[],clients=new Set();

/* ---------- događaji i push prema GUI-ju ---------- */
function ev(level,msg){log.unshift({t:Date.now(),level,msg});log.length=Math.min(log.length,200);push();
  if(level==='error'&&process.env.ALERT_WEBHOOK)fetch(process.env.ALERT_WEBHOOK,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({text:msg})}).catch(()=>{})}
const view=()=>({devices:db.devices.map(d=>({...d,stat:stats[d.id]||{},jobId:busy.get(d.id)||null})),groups:db.groups,playlists:db.playlists,
  files:fs.readdirSync(UP),jobs:[...jobs.values()].map(j=>({id:j.id,label:j.label,prio:j.prio,devices:j.devices,now:j.now})),events:log.slice(0,100)});
function push(){const m=JSON.stringify({type:'state',...view()});clients.forEach(c=>c.readyState===1&&c.send(m))}

/* ---------- MP3 -> RTP (RFC 2250, PT 14) ---------- */
const sock=dgram.createSocket('udp4');sock.bind(()=>{try{sock.setMulticastTTL(16)}catch{}});sock.on('error',e=>ev('error','UDP: '+e.message));
const BR=[0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0],SR=[44100,48000,32000];
function parser(onFrame){let buf=Buffer.alloc(0);return c=>{buf=Buffer.concat([buf,c]);let i=0;
  while(buf.length-i>=4){
    if(buf[i]===0xFF&&(buf[i+1]&0xFE)===0xFA){const br=BR[buf[i+2]>>4],sr=SR[(buf[i+2]>>2)&3];
      if(!br||!sr){i++;continue}
      const len=Math.floor(144000*br/sr)+((buf[i+2]>>1)&1);if(buf.length-i<len)break;
      onFrame(buf.subarray(i,i+len));i+=len}else i++}
  buf=buf.subarray(i)}}
function rtp(job,frame){const p=Buffer.alloc(16+frame.length);p[0]=0x80;p[1]=14;
  p.writeUInt16BE(job.seq=(job.seq+1)&0xFFFF,2);p.writeUInt32BE(job.ts>>>0,4);p.writeUInt32BE(job.ssrc,8);frame.copy(p,16);
  job.ts+=2351; /* 1152 uzoraka @44.1k na 90 kHz satu */
  for(const d of job.dests)sock.send(p,d.port,d.host)}
function item(job,it){return new Promise(res=>{
  const a=['-hide_banner','-loglevel','error',...(it.live?['-fflags','nobuffer']:['-re']),'-i',it.input,'-vn','-ar','44100','-ac','2',
    '-c:a','libmp3lame','-b:a','128k','-write_xing','0','-id3v2_version','0','-f','mp3','pipe:1'];
  const p=spawn('ffmpeg',a,{stdio:[it.live?'pipe':'ignore','pipe','pipe']});job.proc=p;
  p.stdin&&p.stdin.on('error',()=>{});p.stdout.on('data',parser(f=>rtp(job,f)));
  p.stderr.on('data',d=>ev('warn',`ffmpeg (${it.name}): ${String(d).trim().slice(0,200)}`));
  p.on('error',e=>{ev('error','ffmpeg nije dostupan: '+e.message);res()});p.on('close',res)})}
async function run(job,items,loop){let quick=0;
  do{for(const it of items){if(job.stopped)break;job.now=it.name;push();const t=Date.now();await item(job,it);
    if(loop&&Date.now()-t<300&&++quick>5){job.stopped=true;ev('error','Petlja prekinuta: zapisi se ne mogu pustiti')}}}
  while(loop&&!job.stopped);finish(job)}
function release(job){for(const id of job.devices)if(busy.get(id)===job.id)busy.delete(id)}
function finish(job){release(job);if(jobs.delete(job.id)){push()}}
function stop(job){job.stopped=true;release(job);try{job.proc&&job.proc.kill('SIGKILL')}catch{};if(job.proc&&job.proc.stdin)try{job.proc.stdin.end()}catch{};finish(job)}

/* ---------- pokretanje s prioritetima ----------
   3 = pojedinačna obavijest, 2 = grupna obavijest, 1 = playlista (pozadina).
   Novi posao preuzima uređaj samo od nižeg prioriteta; zauzete uređaje grupa preskače. */
function start(target,source,loop){
  let g=null,devs,tname;
  if(target.type==='group'){g=db.groups.find(x=>x.id===target.id);if(!g)throw E(404,'Grupa ne postoji');
    devs=g.deviceIds.map(id=>db.devices.find(d=>d.id===id)).filter(Boolean);tname=g.name}
  else{const d=db.devices.find(x=>x.id===target.id);if(!d)throw E(404,'Uređaj ne postoji');devs=[d];tname=d.name}
  const prio=source.type==='playlist'?1:g?2:3;
  const free=[],skipped=[];
  for(const d of devs){const h=jobs.get(busy.get(d.id));(h&&h.prio>=prio?skipped:free).push(d)}
  if(!free.length)throw E(409,'Zauzeto: '+skipped.map(d=>d.name).join(', '));
  let items,sname;
  if(source.type==='file'){const f=path.basename(source.name);if(!fs.existsSync(path.join(UP,f)))throw E(404,'Datoteka ne postoji');
    items=[{name:f,input:path.join(UP,f)}];sname=f}
  else if(source.type==='playlist'){const pl=db.playlists.find(p=>p.id===source.id);if(!pl||!pl.tracks.length)throw E(404,'Playlista je prazna');
    items=pl.tracks.map(t=>/^https?:/.test(t)?{name:t,input:t}:{name:path.basename(t),input:path.join(UP,path.basename(t))});sname=pl.name}
  else{items=[{name:'Uživo',input:'pipe:0',live:true}];sname='Uživo';loop=false}
  for(const d of free){const h=jobs.get(busy.get(d.id));if(h){ev('info',`„${h.label}“ prekinut zbog višeg prioriteta`);stop(h)}}
  const multi=g&&g.multicast&&!skipped.length;
  const job={id:uid(),prio,label:`${sname} → ${tname}`,devices:free.map(d=>d.id),seq:Math.floor(Math.random()*65535),ts:Math.floor(Math.random()*2**31),
    ssrc:Math.floor(Math.random()*2**32),dests:multi?[{host:g.multicast,port:g.port||4444}]:free.map(d=>({host:d.ip,port:d.port||4444})),loop};
  jobs.set(job.id,job);free.forEach(d=>busy.set(d.id,job.id));
  if(skipped.length)ev('warn',`Preskočeno (zauzeto višim prioritetom): ${skipped.map(d=>d.name).join(', ')}`);
  ev('info',`Start: ${job.label}${multi?' (multicast '+g.multicast+')':''}`);run(job,items,loop);return {job,skipped:skipped.map(d=>d.name)}}

/* ---------- SNMP nadzor ---------- */
const OIDS=['1.3.6.1.2.1.1.1.0','1.3.6.1.2.1.1.3.0','1.3.6.1.2.1.2.2.1.10.1','1.3.6.1.2.1.2.2.1.16.1'];
function poll(d){const s=snmp.createSession(d.ip,d.community||'public',{timeout:2000,retries:0});
  s.on('error',()=>{});
  s.get(OIDS,(err,vb)=>{try{s.close()}catch{};const st=stats[d.id]=stats[d.id]||{fails:0,online:null};
    if(err){st.fails++;if(st.fails>=3&&st.online!==false){st.online=false;ev('error',`Uređaj „${d.name}“ (${d.ip}) ne odgovara na SNMP`)}}
    else{if(st.online===false)ev('info',`Uređaj „${d.name}“ je ponovno dostupan`);st.fails=0;st.online=true;st.seen=Date.now();
      const v=i=>vb[i]&&!snmp.isVarbindError(vb[i])?vb[i].value:null;
      st.descr=v(0)&&String(v(0));st.uptime=v(1)!=null?Math.floor(v(1)/100):null;st.rx=v(2);st.tx=v(3)}
    push()})}
setInterval(()=>db.devices.forEach(d=>d.snmp!==false&&poll(d)),+process.env.POLL_MS||30000);
setTimeout(()=>db.devices.forEach(d=>d.snmp!==false&&poll(d)),2000);

/* ---------- REST ---------- */
const app=express();app.use(express.json());app.use(express.static(path.join(__dirname,'public')));
const wrap=f=>(q,r)=>{try{r.json(f(q)||{ok:1})}catch(e){r.status(e.code||400).json({error:e.message})}};
const upload=multer({storage:multer.diskStorage({destination:UP,filename:(q,f,cb)=>cb(null,path.basename(Buffer.from(f.originalname,'latin1').toString('utf8')))}),limits:{fileSize:300e6}});
app.get('/api/state',wrap(()=>view()));
const crud=(key,pick)=>{
  app.post(`/api/${key}`,wrap(q=>{const o={id:uid(),...pick(q.body)};db[key].push(o);save();push();return o}));
  app.put(`/api/${key}/:id`,wrap(q=>{const o=db[key].find(x=>x.id===q.params.id);if(!o)throw E(404,'Ne postoji');Object.assign(o,pick(q.body));save();push()}));
  app.delete(`/api/${key}/:id`,wrap(q=>{db[key]=db[key].filter(x=>x.id!==q.params.id);
    if(key==='devices')db.groups.forEach(g=>g.deviceIds=g.deviceIds.filter(i=>i!==q.params.id));save();push()}))};
crud('devices',b=>{if(!/^[\w.-]+$/.test(b.ip||''))throw E(400,'Neispravna IP adresa');return {name:String(b.name||b.ip),ip:b.ip,port:+b.port||4444,community:b.community||'public',snmp:b.snmp!==false}});
crud('groups',b=>{if(b.multicast&&!/^2(2[4-9]|3\d)\.\d+\.\d+\.\d+$/.test(b.multicast))throw E(400,'Multicast adresa mora biti 224.0.0.0–239.255.255.255');
  return {name:String(b.name||'Grupa'),multicast:b.multicast||'',port:+b.port||4444,deviceIds:b.deviceIds||[]}});
crud('playlists',b=>({name:String(b.name||'Playlista'),tracks:(b.tracks||[]).map(String)}));
app.post('/api/files',upload.array('f'),wrap(()=>{push()}));
app.delete('/api/files/:n',wrap(q=>{fs.rmSync(path.join(UP,path.basename(q.params.n)),{force:true});push()}));
app.get('/api/files/:n',(q,r)=>r.sendFile(path.resolve(UP,path.basename(q.params.n))));
app.post('/api/playlists/import',upload.single('f'),wrap(q=>{
  const t=fs.readFileSync(q.file.path,'utf8').split(/\r?\n/).map(s=>s.trim()).filter(s=>s&&!s.startsWith('#')).map(s=>/^https?:/.test(s)?s:s.split(/[\\/]/).pop());
  fs.rmSync(q.file.path);const o={id:uid(),name:q.file.originalname.replace(/\.m3u8?$/i,''),tracks:t};db.playlists.push(o);save();push();return o}));
app.post('/api/play',wrap(q=>{const {job,skipped}=start(q.body.target,q.body.source,!!q.body.loop);return {job:job.id,skipped}}));
app.post('/api/stop',wrap(q=>{for(const j of [...jobs.values()])if(q.body.all||j.id===q.body.jobId)stop(j)}));

/* ---------- WebSocket: stanje + mikrofon uživo ---------- */
const server=http.createServer(app),wss=new WebSocketServer({noServer:true});
server.on('upgrade',(q,s,h)=>wss.handleUpgrade(q,s,h,ws=>{const u=new URL(q.url,'http://x');
  if(u.pathname==='/ws/state'){clients.add(ws);ws.on('close',()=>clients.delete(ws));ws.send(JSON.stringify({type:'state',...view()}))}
  else if(u.pathname==='/ws/live'){const [type,id]=(u.searchParams.get('target')||'').split(':');
    try{const {job}=start({type,id},{type:'live'},false);ws.send(JSON.stringify({job:job.id}));
      ws.on('message',m=>{const p=job.proc;if(p&&p.stdin&&p.stdin.writable)p.stdin.write(m)});ws.on('close',()=>stop(job))}
    catch(e){ws.send(JSON.stringify({error:e.message}));ws.close()}}
  else ws.close()}));
server.listen(process.env.PORT||8080,()=>console.log('Exstreamer Hub na portu',process.env.PORT||8080));
