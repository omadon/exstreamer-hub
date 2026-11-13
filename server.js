const express=require('express'),http=require('http'),path=require('path'),fs=require('fs'),dgram=require('dgram');
const {spawn}=require('child_process'),multer=require('multer'),{WebSocketServer}=require('ws'),snmp=require('net-snmp');
const DATA=process.env.DATA_DIR||'./data',UP=path.join(DATA,'uploads'),DB=path.join(DATA,'db.json');
fs.mkdirSync(UP,{recursive:true});
let db={devices:[],groups:[],playlists:[],locations:[]};try{db=JSON.parse(fs.readFileSync(DB))}catch{}db.locations=db.locations||[];
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
const view=ro=>({devices:db.devices.map(d=>({...d,community:ro==='admin'?d.community:undefined,stat:stats[d.id]||{},...alarmInfo(d),jobId:busy.get(d.id)||null})),groups:db.groups,playlists:db.playlists,alarmDefault:process.env.ALARM_DEFAULT||'',locations:db.locations,
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

/* ---------- alarm: zadano iz compose-a, po uređaju: zadano / vlastiti zapis / isključen ---------- */
function defaultSpec(d){let m={};try{m=JSON.parse(process.env.ALARM_DEFAULT_MAP||'{}')}catch{}return m[d.name]||m[d.ip]||process.env.ALARM_DEFAULT||''}
function specSource(spec){if(spec.startsWith('/'))return {type:'path',path:spec};
  const pl=db.playlists.find(p=>p.name===spec);return pl?{type:'playlist',id:pl.id}:{type:'file',name:spec}}
function alarmFor(d){
  if(d.alarm==='none')return null;
  if(d.alarm){const [t,v]=[d.alarm.split(':')[0],d.alarm.slice(d.alarm.indexOf(':')+1)];
    return {mode:'custom',source:t==='file'?{type:'file',name:v}:{type:'playlist',id:v},label:t==='file'?v:(db.playlists.find(p=>p.id===v)||{name:'?'}).name}}
  const spec=defaultSpec(d);return spec?{mode:'default',source:specSource(spec),label:spec.split('/').pop()}:null}
const alarmInfo=d=>{const a=alarmFor(d);return {alarmMode:a?a.mode:'none',alarmLabel:a?a.label:'',alarmDefault:defaultSpec(d).split('/').pop()}};

/* ---------- pokretanje s prioritetima ----------
   3 = pojedinačna obavijest, 2 = grupna obavijest, 1 = playlista (pozadina).
   Novi posao preuzima uređaj samo od nižeg prioriteta; zauzete uređaje grupa preskače. */
function start(target,source,loop,user,alarm){
  let g=null,devs,tname;
  if(target.type==='group'){g=db.groups.find(x=>x.id===target.id);if(!g)throw E(404,'Grupa ne postoji');
    devs=g.deviceIds.map(id=>db.devices.find(d=>d.id===id)).filter(Boolean);tname=g.name}
  else{const d=db.devices.find(x=>x.id===target.id);if(!d)throw E(404,'Uređaj ne postoji');devs=[d];tname=d.name}
  const prio=alarm?4:source.type==='playlist'?1:g?2:3;   /* 4 = alarm: prekida sve */
  const free=[],skipped=[];
  for(const d of devs){const h=jobs.get(busy.get(d.id));(h&&h.prio>=prio?skipped:free).push(d)}
  if(!free.length)throw E(409,'Zauzeto: '+skipped.map(d=>d.name).join(', '));
  let items,sname;
  if(source.type==='file'){const f=path.basename(source.name);if(!fs.existsSync(path.join(UP,f)))throw E(404,'Datoteka ne postoji');
    items=[{name:f,input:path.join(UP,f)}];sname=f}
  else if(source.type==='playlist'){const pl=db.playlists.find(p=>p.id===source.id);if(!pl||!pl.tracks.length)throw E(404,'Playlista je prazna');
    items=pl.tracks.map(t=>/^https?:/.test(t)?{name:t,input:t}:{name:path.basename(t),input:path.join(UP,path.basename(t))});sname=pl.name}
  else if(source.type==='path'){const p=source.path;if(!fs.existsSync(p))throw E(404,'Alarm datoteka ne postoji: '+p);
    if(/\.m3u8?$/i.test(p)){const dir=path.dirname(p);
      items=fs.readFileSync(p,'utf8').split(/\r?\n/).map(l=>l.trim()).filter(l=>l&&!l.startsWith('#')).map(l=>/^https?:/.test(l)?{name:l,input:l}
        :{name:path.basename(l.replace(/\\/g,'/')),input:l.startsWith('/')?l:/^[a-z]:/i.test(l)?path.join(dir,l.split(/[\\/]/).pop()):path.join(dir,l.replace(/\\/g,'/'))});
      if(!items.length)throw E(404,'Alarm playlista je prazna')}
    else items=[{name:path.basename(p),input:p}];sname=path.basename(p)}
  else{items=[{name:'Uživo',input:'pipe:0',live:true}];sname='Uživo';loop=false}
  for(const d of free){const h=jobs.get(busy.get(d.id));if(h){ev('info',`„${h.label}“ prekinut zbog višeg prioriteta`,user);stop(h)}}
  const multi=g&&g.multicast&&!skipped.length;
  const job={id:uid(),t0:Date.now(),prio,label:`${alarm?'ALARM: ':''}${sname} → ${tname}`,devices:free.map(d=>d.id),seq:Math.floor(Math.random()*65535),ts:Math.floor(Math.random()*2**31),
    ssrc:Math.floor(Math.random()*2**32),dests:multi?[{host:g.multicast,port:g.port||4444}]:free.map(d=>({host:d.ip,port:d.port||4444})),loop};
  jobs.set(job.id,job);free.forEach(d=>busy.set(d.id,job.id));
  if(skipped.length)ev('warn',`Preskočeno (zauzeto višim prioritetom): ${skipped.map(d=>d.name).join(', ')}`,user);
  ev(alarm?'warn':'info',`Start: ${job.label}${multi?' (multicast '+g.multicast+')':''}${loop?' [petlja]':''}`,user);run(job,items,loop);return {job,skipped:skipped.map(d=>d.name)}}

/* ---------- VU metar: razina signala po poslu, 10x u sekundi ---------- */
setInterval(()=>{if(!jobs.size||!clients.size)return;const l={};
  for(const j of jobs.values()){const v=j.lv||0;j.lv=0;l[j.id]=Math.max(0,Math.min(1,(20*Math.log10(v+1e-6)+60)/60))}
  const m=JSON.stringify({type:'vu',l});clients.forEach((ro,c)=>c.readyState===1&&c.send(m))},100);

/* ---------- SNMP nadzor (SNMP v1; Barix OID-ovi iz BARIXAUDIOSNMP-MIB, sve je read-only) ----------
   Dostupnost: odgovor na sysDescr/sysUpTime. Stanje zvuka: audioState, buffer, gubitak okvira, soft-errori.
   Dok se na uređaj nešto pušta, upit je svakih 5 s, inače svakih POLL_MS (30 s). Praćenje se gasi po uređaju (snmp:false). */
const B='1.3.6.1.4.1.17491.2.1.';
const O={descr:'1.3.6.1.2.1.1.1.0',up:'1.3.6.1.2.1.1.3.0',name:'1.3.6.1.2.1.1.5.0',loc:'1.3.6.1.2.1.1.6.0',oper:'1.3.6.1.2.1.2.2.1.8.1',udpIn:'1.3.6.1.2.1.7.1.0',
  left:B+'1.1.0',right:B+'1.2.0',buf:B+'2.1.0',loss:B+'2.2.0',soft:B+'2.3.0',stream:B+'3.1.0',bitrate:B+'3.3.0',reconn:B+'3.4.0',err:B+'3.6.0',errT:B+'3.7.0'};
const ERR={0:'bez greške',1:'DNS problem',2:'nema TCP odgovora',3:'TCP zatvoren',4:'nema HTTP odgovora',5:'neispravan HTTP odgovor',11:'istek veze',13:'previše ispadanja',16:'greška playliste',17:'prazan URL',19:'nepodržan format zvuka',99:'interna greška'};
const errName=c=>ERR[c]||(c>=400&&c<600?'HTTP '+c:'kod '+c);
const sess1=d=>{const s=snmp.createSession(d.ip,d.community||'public',{version:snmp.Version1,timeout:2000,retries:1});s.on('error',()=>{});return s};
const val=(vb,i)=>vb&&vb[i]&&!snmp.isVarbindError(vb[i])?vb[i].value:null;
const txt=x=>x==null?null:Buffer.isBuffer(x)?x.toString():x;
const sget=(s,o)=>new Promise(r=>s.get(o,(e,v)=>r(e?null:v)));   /* v1: jedan nepodržan OID ruši cijeli upit, zato zasebni upiti */
async function poll(d){const st=stats[d.id]=stats[d.id]||{fails:0,online:null},now=Date.now();if(st.polling)return;st.polling=true;st.last=now;const s=sess1(d);
  try{
    const vb=await sget(s,[O.descr,O.up,O.name,O.loc,O.oper]);
    if(!vb){st.fails++;if(st.fails>=3&&st.online!==false){st.online=false;ev('error',`Uređaj „${d.name}“ (${d.ip}) ne odgovara na SNMP`)}return}
    if(st.online===false)ev('info',`Uređaj „${d.name}“ je ponovno dostupan`);
    st.fails=0;st.online=true;st.seen=now;st.descr=txt(val(vb,0));st.fw=(/Snr:?\s*(\S+)/i.exec(st.descr||'')||[])[1]||null;
    st.uptime=val(vb,1)!=null?Math.floor(val(vb,1)/100):null;st.sysName=txt(val(vb,2));st.sysLoc=txt(val(vb,3));st.oper=val(vb,4);
    const v2=await sget(s,[O.udpIn]);
    if(v2){const u=val(v2,0);if(u!=null){if(st.udpIn!=null&&st.udpAt&&u>=st.udpIn)st.pps=Math.round((u-st.udpIn)/((now-st.udpAt)/1000)*10)/10;st.udpIn=u;st.udpAt=now}}
    const v3=await sget(s,[O.left,O.right,O.buf,O.loss,O.soft,O.stream,O.bitrate,O.reconn,O.err,O.errT]);
    if(v3){const n=i=>val(v3,i);st.audio=Math.max(n(0)||0,n(1)||0);st.buf=n(2);st.loss=n(3);st.soft=n(4);st.streamNo=n(5);st.bitrate=n(6);st.reconn=n(7);
      if(st.errT!=null&&n(9)!=null&&n(9)!==st.errT&&busy.has(d.id))ev('warn',`Uređaj „${d.name}“ javlja grešku streama: ${errName(n(8))}`);
      st.err=n(8);st.errT=n(9)}
    checkStream(d,st)}
  finally{try{s.close()}catch{}st.polling=false;push()}}
function checkStream(d,st){const j=jobs.get(busy.get(d.id));
  if(!j){st.low=0;st.jb=null;return}
  if(!st.jb||st.jb.id!==j.id)st.jb={id:j.id,loss:st.loss,soft:st.soft};const b=st.jb;
  if(st.loss!=null&&b.loss!=null&&st.loss-b.loss>=25&&!b.wl){b.wl=1;ev('warn',`Uređaj „${d.name}“ je izgubio ${st.loss-b.loss} audio okvira tijekom „${j.label}“ (provjeri mrežu)`)}
  if(st.soft!=null&&b.soft!=null&&st.soft-b.soft>=10&&!b.ws){b.ws=1;ev('warn',`Uređaj „${d.name}“: buffer prazan ili gubitak veći od korekcije (${st.soft-b.soft}×) tijekom „${j.label}“`)}
  if(Date.now()-j.t0<12000||st.pps==null)return;
  if(st.pps<5){if(++st.low===2&&st.warned!==j.id){st.warned=j.id;ev('error',`Uređaj „${d.name}“ ne prima audio tok (${st.pps} pkt/s) dok se pušta „${j.label}“. Provjeri mrežu i multicast članstvo.`)}}else st.low=0}
setInterval(()=>{const n=Date.now();db.devices.forEach(d=>{if(d.snmp===false){delete stats[d.id];return}
  const st=stats[d.id],every=busy.has(d.id)?5000:(+process.env.POLL_MS||30000);if(!st||!st.last||n-st.last>=every-200)poll(d)})},2500);

/* ---------- SNMP dijagnostika: koji se brojači mijenjaju dok se zvuk pušta ---------- */
const walk=(d,base)=>new Promise(res=>{const out={},s=sess1(d);
  s.subtree(base,vbs=>{for(const v of vbs)if(!snmp.isVarbindError(v))out[v.oid]=txt(v.value)},()=>{try{s.close()}catch{}res(out)})});
const snap=async d=>Object.assign({},...await Promise.all(['1.3.6.1.4.1.17491','1.3.6.1.2.1.7','1.3.6.1.2.1.2.2'].map(b=>walk(d,b))));

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
/* javno: podaci za zaglavlje i logo (potrebni i prije prijave) */
const LOGO=process.env.LOGO_PATH,hasLogo=()=>{try{return !!LOGO&&fs.statSync(LOGO).isFile()}catch{return false}};
app.get('/logo',(q,r)=>hasLogo()?(r.setHeader('Cache-Control','no-cache'),r.sendFile(path.resolve(LOGO))):r.status(404).end());
app.get('/api/about',(q,r)=>r.json({company:process.env.ABOUT_COMPANY||'',description:process.env.ABOUT_DESCRIPTION||'',name:process.env.CONTACT_NAME||'',
  email:process.env.CONTACT_EMAIL||'',phone:process.env.CONTACT_PHONE||'',logo:hasLogo()}));
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
  if(q.method!=='GET'){const adminOnly=!['/play','/stop','/alarm'].includes(q.path);
    if(adminOnly&&x.role!=='admin'){ev('warn',`Odbijeno (nema ovlasti): ${q.method} ${q.originalUrl}`,x.user);return r.status(403).json({error:'Samo administrator može mijenjati uređaje, grupe, zapise i playliste'})}
    const m=/^\/(devices|groups|playlists|locations)\/(\w+)/.exec(q.path),o=m&&db[m[1]].find(z=>z.id===m[2]),nm=o?o.name:(q.body&&(q.body.name||q.body.ip))||'';
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
    if(key==='devices')db.groups.forEach(g=>g.deviceIds=g.deviceIds.filter(i=>i!==q.params.id));if(key==='locations')db.devices.forEach(d=>{if(d.locationId===q.params.id)d.locationId=''});save();push()}))};
crud('devices',b=>{if(!/^[\w.-]+$/.test(b.ip||''))throw E(400,'Neispravna IP adresa');return {name:String(b.name||b.ip),ip:b.ip,port:+b.port||4444,community:b.community||'public',snmp:b.snmp!==false,alarm:String(b.alarm||''),locationId:db.locations.some(l=>l.id===b.locationId)?b.locationId:''}});
crud('groups',b=>{if(b.multicast&&!/^2(2[4-9]|3\d)\.\d+\.\d+\.\d+$/.test(b.multicast))throw E(400,'Multicast adresa mora biti 224.0.0.0–239.255.255.255');
  return {name:String(b.name||'Grupa'),multicast:b.multicast||'',port:+b.port||4444,deviceIds:b.deviceIds||[]}});
crud('locations',b=>{const n=String(b.name||'').trim();if(!n)throw E(400,'Naziv lokacije je obavezan');return {name:n}});
crud('playlists',b=>({name:String(b.name||'Playlista'),tracks:(b.tracks||[]).map(String)}));
app.post('/api/files',upload.array('f'),wrap(()=>{push()}));
app.delete('/api/files/:n',wrap(q=>{fs.rmSync(path.join(UP,path.basename(q.params.n)),{force:true});push()}));
app.get('/api/files/:n',(q,r)=>r.sendFile(path.resolve(UP,path.basename(q.params.n))));
app.post('/api/playlists/import',upload.single('f'),wrap(q=>{
  const t=fs.readFileSync(q.file.path,'utf8').split(/\r?\n/).map(s=>s.trim()).filter(s=>s&&!s.startsWith('#')).map(s=>/^https?:/.test(s)?s:s.split(/[\\/]/).pop());
  fs.rmSync(q.file.path);const o={id:uid(),name:q.file.originalname.replace(/\.m3u8?$/i,''),tracks:t};db.playlists.push(o);save();push();return o}));
app.post('/api/play',wrap(q=>{if(!q.body.source||q.body.source.type==='path')throw E(400,'Neispravan izvor');const {job,skipped}=start(q.body.target,q.body.source,!!q.body.loop,q.user);return {job:job.id,skipped}}));
app.post('/api/alarm',wrap(q=>{const d=db.devices.find(x=>x.id===q.body.id);if(!d)throw E(404,'Uređaj ne postoji');
  const a=alarmFor(d);if(!a)throw E(400,'Alarm nije postavljen ili je isključen za ovaj uređaj');
  return {job:start({type:'device',id:d.id},a.source,false,q.user,true).job.id}}));
app.get('/favicon.ico',(q,r)=>r.sendFile(path.join(__dirname,'public','apple-touch-icon.png')));
app.post('/api/devices/:id/snmp-diff',async(q,r)=>{try{const d=db.devices.find(x=>x.id===q.params.id);if(!d)throw E(404,'Uređaj ne postoji');
  const sec=Math.min(60,Math.max(5,+q.body.seconds||15)),a=await snap(d);if(!Object.keys(a).length)throw E(504,'Uređaj ne odgovara na SNMP');
  await new Promise(z=>setTimeout(z,sec*1000));const b=await snap(d),ch=[];
  for(const o in b)if(typeof b[o]==='number'&&typeof a[o]==='number'&&a[o]!==b[o])ch.push({oid:o,before:a[o],after:b[o],delta:b[o]-a[o]});
  ev('info',`SNMP dijagnostika „${d.name}“: ${ch.length} brojača se promijenilo u ${sec} s`,q.user);r.json({seconds:sec,total:Object.keys(b).length,changes:ch})}
  catch(e){r.status(e.code||500).json({error:e.message})}});
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
