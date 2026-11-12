const express=require('express'),http=require('http'),path=require('path'),fs=require('fs'),dgram=require('dgram');
const {spawn}=require('child_process'),multer=require('multer'),{WebSocketServer}=require('ws'),snmp=require('net-snmp');
const DATA=process.env.DATA_DIR||'./data',UP=path.join(DATA,'uploads'),DB=path.join(DATA,'db.json');
fs.mkdirSync(UP,{recursive:true});
let db={devices:[],groups:[],playlists:[]};try{db=JSON.parse(fs.readFileSync(DB))}catch{}
const save=()=>fs.writeFileSync(DB,JSON.stringify(db,null,1)),uid=()=>Math.random().toString(36).slice(2,9);
const E=(c,m)=>Object.assign(new Error(m),{code:c});
const os=require('os'),net=require('net');
const SH=process.env.SYSLOG_HOST,SP=+process.env.SYSLOG_PORT||514,STCP=(process.env.SYSLOG_PROTO||'udp')==='tcp',su=dgram.createSocket('udp4');let st=null;su.on('error',()=>{});
function syslog(level,msg,user){if(!SH)return;const sev={error:3,warn:4,info:6}[level]||6;
  const m=`<${(+process.env.SYSLOG_FACILITY||16)*8+sev}>1 ${new Date().toISOString()} ${os.hostname()} exstreamer-hub ${process.pid} ${user?'AUDIT':'EVENT'} - ${user?'user='+user+' ':''}${String(msg).replace(/[\r\n]+/g,' ')}`;
  if(STCP){const b=Buffer.from(m),f=Buffer.concat([Buffer.from(b.length+' '),b]);
    if(!st||st.destroyed){st=net.connect(SP,SH);st.on('error',()=>{});st.on('close',()=>{st=null})}st.write(f)}
  else su.send(m,SP,SH)}
const stats={},jobs=new Map(),busy=new Map(),log=[],clients=new Map();

/* ---------- događaji i push prema GUI-ju ---------- */
function ev(level,msg,user){log.unshift({t:Date.now(),level,msg,user});log.length=Math.min(log.length,200);syslog(level,msg,user);console.log(`[${level}] ${user?user+': ':''}${msg}`);push();
  if(level==='error'&&process.env.ALERT_WEBHOOK)fetch(process.env.ALERT_WEBHOOK,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({text:(user?user+': ':'')+msg})}).catch(()=>{})}
const view=ro=>({devices:db.devices.map(d=>({...d,community:ro==='admin'?d.community:undefined,stat:stats[d.id]||{},jobId:busy.get(d.id)||null})),groups:db.groups,playlists:db.playlists,
  files:fs.readdirSync(UP),jobs:[...jobs.values()].map(j=>({id:j.id,label:j.label,prio:j.prio,devices:j.devices,now:j.now})),events:log.slice(0,100)});
function push(){clients.forEach((ro,c)=>c.readyState===1&&c.send(JSON.stringify({type:'state',...view(ro)})))}

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
  const a=['-hide_banner','-loglevel','error',...(it.live?['-fflags','nobuffer']:['-re']),'-i',it.input,'-map','0:a','-ar','44100','-ac','2',
    '-c:a','libmp3lame','-b:a','128k','-write_xing','0','-id3v2_version','0','-f','mp3','pipe:1','-map','0:a','-ac','1','-ar','8000','-f','s16le','pipe:3'];
  const p=spawn('ffmpeg',a,{stdio:[it.live?'pipe':'ignore','pipe','pipe','pipe']});job.proc=p;
  p.stdin&&p.stdin.on('error',()=>{});
  p.stdio[3].on('data',b=>{let q=0;const n=b.length>>1;for(let i=0;i<n;i++){const v=b.readInt16LE(i*2);q+=v*v}job.lv=Math.max(job.lv||0,Math.sqrt(q/Math.max(n,1))/32768)});p.stdio[3].on('error',()=>{});
  p.stdout.on('data',parser(f=>rtp(job,f)));
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
function start(target,source,loop,user){
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
  for(const d of free){const h=jobs.get(busy.get(d.id));if(h){ev('info',`„${h.label}“ prekinut zbog višeg prioriteta`,user);stop(h)}}
  const multi=g&&g.multicast&&!skipped.length;
  const job={id:uid(),prio,label:`${sname} → ${tname}`,devices:free.map(d=>d.id),seq:Math.floor(Math.random()*65535),ts:Math.floor(Math.random()*2**31),
    ssrc:Math.floor(Math.random()*2**32),dests:multi?[{host:g.multicast,port:g.port||4444}]:free.map(d=>({host:d.ip,port:d.port||4444})),loop};
  jobs.set(job.id,job);free.forEach(d=>busy.set(d.id,job.id));
  if(skipped.length)ev('warn',`Preskočeno (zauzeto višim prioritetom): ${skipped.map(d=>d.name).join(', ')}`,user);
  ev('info',`Start: ${job.label}${multi?' (multicast '+g.multicast+')':''}${loop?' [petlja]':''}`,user);run(job,items,loop);return {job,skipped:skipped.map(d=>d.name)}}

/* ---------- VU metar: razina signala po poslu, 10x u sekundi ---------- */
setInterval(()=>{if(!jobs.size||!clients.size)return;const l={};
  for(const j of jobs.values()){const v=j.lv||0;j.lv=0;l[j.id]=Math.max(0,Math.min(1,(20*Math.log10(v+1e-6)+60)/60))}
  const m=JSON.stringify({type:'vu',l});clients.forEach((ro,c)=>c.readyState===1&&c.send(m))},100);

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
const app=express();app.use(express.json());app.use(express.static(path.join(__dirname,'public'),{setHeaders:r=>r.setHeader('Cache-Control','no-cache')}));
/* ---------- prijava: operatori preko Active Directoryja (LDAP), admin lozinkom iz konfiguracije ---------- */
app.set('trust proxy',true);
const sess=new Map(),ADM=process.env.ADMIN_PASSWORD||'admin';
if(ADM==='admin')console.warn('UPOZORENJE: postavi ADMIN_PASSWORD u docker-compose.yml');
if(!process.env.LDAP_URL)console.warn('LDAP_URL nije postavljen: prijava je moguća samo administratoru');
const sessOf=q=>{const m=/(?:^|; )sid=(\w+)/.exec(q.headers.cookie||''),x=m&&sess.get(m[1]);if(x&&x.exp<Date.now()){sess.delete(m[1]);return null}return x||null};
const role=q=>(sessOf(q)||{}).role||null;
const lesc=v=>v.replace(/[\\*()\0]/g,c=>'\\'+c.charCodeAt(0).toString(16).padStart(2,'0'));
async function ldapAuth(u,p){
  if(!process.env.LDAP_URL||!process.env.LDAP_DOMAIN)throw new Error('LDAP nije konfiguriran');
  if(!/^[\w.\-]+$/.test(u)||!p)return false;           /* prazna lozinka bi prošla kao anonimni bind */
  const {Client}=require('ldapts'),c=new Client({url:process.env.LDAP_URL,timeout:6000,connectTimeout:6000,tlsOptions:{rejectUnauthorized:process.env.LDAP_TLS_INSECURE!=='1'}});
  try{await c.bind(`${u}@${process.env.LDAP_DOMAIN}`,p);
    if(!process.env.LDAP_BASE_DN)return true;
    const g=process.env.LDAP_GROUP_DN,filter=g?`(&(sAMAccountName=${u})(memberOf:1.2.840.113556.1.4.1941:=${lesc(g)}))`:`(sAMAccountName=${u})`;
    const {searchEntries}=await c.search(process.env.LDAP_BASE_DN,{scope:'sub',filter,attributes:['sAMAccountName']});
    return searchEntries.length>0}
  catch(e){if(/InvalidCredentials/.test(e.name||e.constructor.name))return false;throw e}
  finally{try{await c.unbind()}catch{}}}
app.post('/api/login',async(q,r)=>{const u=String(q.body.username||'').trim(),p=String(q.body.password||'');let ro=null;
  try{if(u.toLowerCase()==='admin'){if(p===ADM)ro='admin'}else if(await ldapAuth(u,p))ro='operator'}
  catch(e){ev('error',`LDAP greška: ${e.message}`);return r.status(503).json({error:'LDAP server nije dostupan'})}
  if(!ro){ev('warn',`Neuspjela prijava: ${u||'(prazno)'} s ${q.ip}`);return setTimeout(()=>r.status(401).json({error:'Pogrešno korisničko ime ili lozinka'}),700)}
  const id=require('crypto').randomBytes(16).toString('hex'),user=ro==='admin'?'admin':u.toLowerCase();
  sess.set(id,{role:ro,user,exp:Date.now()+12*3600e3});ev('info',`Prijava (${ro}) s ${q.ip}`,user);
  r.setHeader('Set-Cookie',`sid=${id}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200`);r.json({role:ro,user})});
app.post('/api/logout',(q,r)=>{const x=sessOf(q),m=/(?:^|; )sid=(\w+)/.exec(q.headers.cookie||'');if(x){ev('info','Odjava',x.user);sess.delete(m[1])}r.setHeader('Set-Cookie','sid=; Path=/; Max-Age=0');r.json({ok:1})});
app.get('/api/me',(q,r)=>{const x=sessOf(q);x?r.json({role:x.role,user:x.user}):r.status(401).json({error:'Prijava je potrebna'})});
app.use('/api',(q,r,n)=>{const x=sessOf(q);if(!x)return r.status(401).json({error:'Prijava je potrebna'});q.role=x.role;q.user=x.user;
  if(q.method!=='GET'){const adminOnly=!['/play','/stop'].includes(q.path);
    if(adminOnly&&x.role!=='admin'){ev('warn',`Odbijeno (nema ovlasti): ${q.method} ${q.originalUrl}`,x.user);return r.status(403).json({error:'Samo administrator može mijenjati uređaje, grupe, zapise i playliste'})}
    const m=/^\/(devices|groups|playlists)\/(\w+)/.exec(q.path),o=m&&db[m[1]].find(z=>z.id===m[2]),nm=o?o.name:(q.body&&(q.body.name||q.body.ip))||'';
    const what=`${q.method} ${q.originalUrl}${nm?' ('+nm+')':''}`;
    r.on('finish',()=>{if(r.statusCode>=400)ev('warn',`Neuspjelo: ${what} → ${r.statusCode}`,x.user);else if(adminOnly)ev('info',what,x.user)})}
  n()});
const wrap=f=>(q,r)=>{try{r.json(f(q)||{ok:1})}catch(e){r.status(e.code||400).json({error:e.message})}};
const upload=multer({storage:multer.diskStorage({destination:UP,filename:(q,f,cb)=>cb(null,path.basename(Buffer.from(f.originalname,'latin1').toString('utf8')))}),limits:{fileSize:300e6}});
app.get('/api/state',wrap(q=>view(q.role)));
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
app.post('/api/play',wrap(q=>{const {job,skipped}=start(q.body.target,q.body.source,!!q.body.loop,q.user);return {job:job.id,skipped}}));
app.post('/api/stop',wrap(q=>{for(const j of [...jobs.values()])if(q.body.all||j.id===q.body.jobId){ev('info','Stop: '+j.label,q.user);stop(j)}}));

/* ---------- WebSocket: stanje + mikrofon uživo ---------- */
const server=http.createServer(app),wss=new WebSocketServer({noServer:true});
server.on('upgrade',(q,s,h)=>wss.handleUpgrade(q,s,h,ws=>{const se=sessOf(q);if(!se)return ws.close();const ro=se.role;const u=new URL(q.url,'http://x');
  if(u.pathname==='/ws/state'){clients.set(ws,ro);ws.on('close',()=>clients.delete(ws));ws.send(JSON.stringify({type:'state',...view(ro)}))}
  else if(u.pathname==='/ws/live'){const [type,id]=(u.searchParams.get('target')||'').split(':');
    try{const {job}=start({type,id},{type:'live'},false,se.user);ws.send(JSON.stringify({job:job.id}));
      ws.on('message',m=>{const p=job.proc;if(p&&p.stdin&&p.stdin.writable)p.stdin.write(m)});ws.on('close',()=>stop(job))}
    catch(e){ws.send(JSON.stringify({error:e.message}));ws.close()}}
  else ws.close()}));
server.listen(process.env.PORT||8080,()=>console.log('Exstreamer Hub na portu',process.env.PORT||8080));
