const nodes=[document.querySelector('#node1'),document.querySelector('#node2'),document.querySelector('#node3')];
const log=document.querySelector('#eventLog');
const quorum=document.querySelector('#quorumBadge');
const status=document.querySelector('#writeStatus');
let mode='normal',term=18,leader=0,sequence=0;
const times=()=>`00:${String(Math.floor(sequence/60)).padStart(2,'0')}.${String((sequence*137)%1000).padStart(3,'0')}`;
function addLog(message,type='info'){
  sequence++;
  const p=document.createElement('p');
  const icons={ok:'✓',info:'→',warn:'!',error:'×'};
  p.innerHTML=`<time>${times()}</time><span class="${type}">${icons[type]}</span>${message}`;
  log.appendChild(p);log.scrollTop=log.scrollHeight;
}
function setLeader(index){
  leader=index;
  nodes.forEach((node,i)=>{
    node.classList.toggle('leader',i===index&&!node.classList.contains('down')&&!node.classList.contains('partitioned'));
    node.classList.toggle('follower',i!==index);
    node.querySelector('.role').textContent=i===index?'LEADER':'FOLLOWER';
    node.querySelector('.term').textContent=term;
  });
}
function resetNodes(){
  nodes.forEach(n=>{n.classList.remove('down','partitioned','lagging');n.querySelector('.node-state').textContent='healthy'});
  ['link12','link13','link23'].forEach(id=>document.querySelector('#'+id).classList.remove('broken'));
  quorum.innerHTML='<span>2 / 3</span> quorum available';status.textContent='Writes are acknowledged after replication to 2 of 3 nodes.';
}
function selectAction(action){
  document.querySelectorAll('.scenario').forEach(b=>b.classList.toggle('active',b.dataset.action===action));
}
function run(action){
  if(action==='normal'){mode='normal';term=18;resetNodes();setLeader(0);addLog('Healthy cluster restored','ok');selectAction(action);return}
  if(action==='crash'){
    resetNodes();mode='crash';nodes[leader].classList.add('down');nodes[leader].querySelector('.node-state').textContent='process stopped';addLog(`node-${leader+1} process stopped`,'error');
    setTimeout(()=>{term++;const next=leader===0?1:0;setLeader(next);addLog(`node-${next+1} elected leader, term ${term}`,'ok');addLog('Two-node quorum restored','ok');},650);selectAction(action);return;
  }
  if(action==='partition'){
    resetNodes();mode='partition';const old=leader;nodes[old].classList.add('partitioned');nodes[old].querySelector('.node-state').textContent='isolated';
    if(old===0){document.querySelector('#link12').classList.add('broken');document.querySelector('#link13').classList.add('broken')}else if(old===1){document.querySelector('#link12').classList.add('broken');document.querySelector('#link23').classList.add('broken')}else{document.querySelector('#link13').classList.add('broken');document.querySelector('#link23').classList.add('broken')}
    addLog(`node-${old+1} isolated from peers`,'warn');addLog('Old leader cannot confirm quorum read','error');
    setTimeout(()=>{term++;let next=[0,1,2].find(i=>i!==old);setLeader(next);nodes[old].querySelector('.role').textContent='OLD LEADER';addLog(`Majority elected node-${next+1}, term ${term}`,'ok')},650);selectAction(action);return;
  }
  if(action==='lag'){
    resetNodes();mode='lag';const lagger=leader===2?1:2;nodes[lagger].classList.add('lagging');nodes[lagger].querySelector('.node-state').textContent='105 entries behind';
    if(lagger===1){document.querySelector('#link12').classList.add('broken');document.querySelector('#link23').classList.add('broken')}else{document.querySelector('#link13').classList.add('broken');document.querySelector('#link23').classList.add('broken')}
    addLog(`node-${lagger+1} replication paused`,'warn');addLog('Leader compacts committed prefix at index 100','info');selectAction(action);return;
  }
  if(action==='heal'){
    const affected=nodes.find(n=>n.classList.contains('down')||n.classList.contains('partitioned')||n.classList.contains('lagging'));resetNodes();mode='normal';setLeader(leader);
    addLog('Network and process health restored','info');
    if(affected){affected.classList.add('lagging');affected.querySelector('.node-state').textContent='installing snapshot';addLog(`${affected.querySelector('strong').textContent} receiving snapshot`,'info');setTimeout(()=>{affected.classList.remove('lagging');affected.querySelector('.node-state').textContent='healthy';addLog('Snapshot installed; log suffix caught up','ok');addLog('All state hashes match','ok')},750)}
    selectAction(action);
  }
}
document.querySelectorAll('.scenario').forEach(b=>b.addEventListener('click',()=>run(b.dataset.action)));
document.querySelector('#resetLog').addEventListener('click',()=>{log.innerHTML='';sequence=0;addLog('Event stream cleared','info')});
document.querySelector('#putButton').addEventListener('click',()=>{
  const key=document.querySelector('#keyInput').value.trim()||'key';const value=document.querySelector('#valueInput').value.trim()||'value';
  if(mode==='partition'&&nodes[leader].classList.contains('partitioned')){addLog('Write redirected to majority leader','warn')}
  const healthy=nodes.filter(n=>!n.classList.contains('down')&&!n.classList.contains('partitioned')).length;
  if(healthy<2){addLog(`PUT ${key} rejected: quorum unavailable`,'error');status.textContent='No majority—write safely rejected.';return}
  addLog(`Appended PUT ${key}=${value} to leader log`,'info');
  setTimeout(()=>{addLog('AppendEntries acknowledged by majority','ok');nodes.filter(n=>!n.classList.contains('down')&&!n.classList.contains('partitioned')&&!n.classList.contains('lagging')).forEach(n=>{const bars=n.querySelectorAll('.log-strip i');bars[Math.floor(Math.random()*bars.length)].classList.add('committed')});status.textContent=`Committed “${key}=${value}” on a 2-of-3 quorum.`},380);
});
const stepData=[
  ['STEP 1 · CLIENT BOUNDARY','Only the leader accepts a mutation.','A follower returns the known leader so the client can retry. A client/request ID makes that retry idempotent instead of creating a duplicate write.','<span class="dim">POST</span> /kv/color\n{\n  <span class="key">"value"</span>: <span class="string">"cyan"</span>,\n  <span class="key">"client_id"</span>: <span class="string">"demo"</span>,\n  <span class="key">"request_id"</span>: <span class="string">"42"</span>\n}'],
  ['STEP 2 · DURABLE LOG','Persist before claiming progress.','The leader appends the command under its current term. State is written to a temporary file, synchronized, and atomically renamed before replication success can be reported.','log[105] = {\n  <span class="key">term</span>: 18,\n  <span class="key">command</span>: <span class="string">"PUT color cyan"</span>\n}\n<span class="dim">fsync → atomic rename</span>'],
  ['STEP 3 · PEER REPLICATION','Followers prove matching history.','AppendEntries includes the preceding index and term. A mismatch makes the leader move that follower backward until the histories align—or install a snapshot if the required prefix was compacted.','AppendEntries {\n  <span class="key">prev_log_index</span>: 104,\n  <span class="key">prev_log_term</span>: 18,\n  <span class="key">entries</span>: [log[105]]\n}'],
  ['STEP 4 · MAJORITY RULE','Two durable copies make it committed.','The leader advances commitIndex after itself and one follower acknowledge the entry. A lone or isolated leader cannot cross this boundary.','acks = [node_1, node_2]\nquorum = <span class="string">2</span>\n\n<span class="key">commitIndex</span> = 105\n<span class="dim">// safe to acknowledge</span>'],
  ['STEP 5 · STATE MACHINE','Apply in the same order everywhere.','Committed entries are applied deterministically to the key-value map and deduplication table. Followers learn the commit index through subsequent heartbeats.','store[<span class="string">"color"</span>] = <span class="string">"cyan"</span>\ndedupe[<span class="string">"demo:42"</span>] = result\n\n<span class="dim">HTTP 201 Created</span>']
];
document.querySelectorAll('.step').forEach(b=>b.addEventListener('click',()=>{const i=Number(b.dataset.step),d=stepData[i];document.querySelectorAll('.step').forEach(x=>x.classList.toggle('active',x===b));document.querySelector('#stepLabel').textContent=d[0];document.querySelector('#stepTitle').textContent=d[1];document.querySelector('#stepCopy').textContent=d[2];document.querySelector('#stepCode').innerHTML=d[3]}));
