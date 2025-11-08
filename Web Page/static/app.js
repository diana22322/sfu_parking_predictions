// static/app.js
(function(){
  //helpers
  function $(sel, root=document){ return root.querySelector(sel); }
  function clamp(n, lo, hi){ return Math.min(Math.max(n, lo), hi); }


  //TO BE DELETED AFTER REAL DATA ->
  //simple seeded PRNG so each lot has its own stats
  function hash32(str){
    let h = 2166136261 >>> 0;
    for (let i=0;i<str.length;i++){
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }
  function rng(seed){
    let s = seed >>> 0;
    return function(){
      // LCG
      s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
      return (s >>> 0) / 0xFFFFFFFF;
    };
  }
  //<- TO BE DELETED AFTER REAL DATA



  // updating the visual state and text of a donut chart to show lot occupancy
  function renderDonut(root, total, occupied){
    const available = Math.max(total - occupied, 0);
    const pct = total > 0 ? clamp((occupied / total) * 100, 0, 100) : 0; //occupancy in %

    let judgement = "EMPTY";
    if (pct === 100)      judgement = "FULL";
    else if (pct >= 90)   judgement = "ALMOST FULL";
    else if (pct >= 70)   judgement = "BUSY";
    else if (pct >= 40)   judgement = "MODERATE";
    else if (pct >= 10)   judgement = "LIGHT";
    else if (pct > 0)     judgement = "VERY LIGHT";

    //quering necessary elements
    const occ = root.querySelector(".donut-occ");
    const free = root.querySelector(".donut-free");
    const totalEl = root.querySelector("#donut-total");
    const availEl = root.querySelector("#available");
    const judgeEl = root.querySelector("#judgement");

    //radius and circumference
    const R = 40, C = 2 * Math.PI * R;
    const occLen  = (pct / 100) * C; //length of occupied stroke
    const freeLen = C - occLen; //length of free stroke

    occ.style.strokeDasharray  = `${occLen} ${C}`;
    occ.style.strokeDashoffset = 0;
    free.style.strokeDasharray  = `${occLen} ${freeLen}`;
    free.style.strokeDashoffset = 0;

    totalEl.textContent = String(total);
    availEl.textContent = String(available);
    judgeEl.textContent = judgement;
  }


  //TO BE MODIFIED AFTER REAL DATA ->
  // lot stats (07:00–21:00, every 2 hours)
  function generateLotStats(lotName){
    const times = ["07:00","09:00","11:00","13:00","15:00","17:00","19:00","21:00"];
    const baseCurve = [20, 40, 60, 80, 72, 55, 65, 25]; 
    const rnd = rng(hash32(lotName));

    const points = baseCurve.map((b, i) => { //iterating over the baseCurve and adding ±6 random noise to each point
      const noise = Math.round((rnd()-0.5) * 12); 
      return { time: times[i], pct: clamp(b + noise, 0, 100) }; //clamp the result to 0-100%
    });


    const avgOcc = Math.round(points.reduce((a,p)=>a+p.pct,0) / points.length); //average occupancy
    const baseMin = 90 + Math.round(rnd()*60); // random avg duration ~ 90–220 min
    const extra   = Math.round((avgOcc/100) * 70);
    const minutes = baseMin + extra; //total avg duration in minutes
    const h = Math.floor(minutes / 60), m = minutes % 60; //converting into hours and remaining minutes

    return {
      points,
      avgOcc,                         // %
      avgDurationLabel: `${h}h ${String(m).padStart(2,'0')}m`
    };
  }
  // <- TO BE MODIFIED AFTER REAL DATA 



  // taking the occupancy points and generating an SVG line chart
  function renderOccupancyChart(root, points){
    const svg = root.querySelector("#occ-chart");
    if(!svg) return;

    const W=320, H=200, padL=36, padR=10, padT=16, padB=44;
    const cw = W - padL - padR, ch = H - padT - padB;

    const times = points.map(p => p.time);
    const n = points.length;

    function x(i){ return padL + (i/(n-1))*cw; }
    function y(pct){ return padT + (100 - pct)/100 * ch; }

    // grid (20%,40%,60%,80%)
    const gridY = [20,40,60,80];
    const gridLines = gridY.map(g => {
      const gy = y(g);
      return `<line class="chart-grid" x1="${padL}" y1="${gy}" x2="${padL+cw}" y2="${gy}"/>`;
    }).join("");

    // x ticks + labels
    const ticks = times.map((t,i)=>{
      const xi = x(i);
      const y0 = padT + ch;
      return `
        <line class="chart-grid" x1="${xi}" y1="${y0}" x2="${xi}" y2="${y0+4}"/>
        <text class="chart-tick" transform="translate(${xi-2},${y0+32}) rotate(-90)">${t}</text>
      `;
    }).join("");

    // path
    const pathD = points.map((p,i)=>`${i?'L':'M'} ${x(i)} ${y(p.pct)}`).join(" ");
    const path = `<path class="chart-line" d="${pathD}" fill="none" />`;

    svg.innerHTML = `
      <g>${gridLines}</g>
      <g>${ticks}</g>
      <g>${path}</g>
    `;
  }

  // fetching lot data and displaying correct info
  async function initCampus(){
    const picker = $(".lot-picker");
    if(!picker) return; // not on campus page

    const campus = picker.getAttribute("data-campus");
    const menu   = $("#lot-menu", picker);
    const title  = $("#selected-lot", picker);
    const lotNameEl = $(".lot-name", title);
    const statusEl  = $("#lot-error", picker);
    const avgDurEl  = $("#avg-duration", picker);
    const avgOccEl  = $("#avg-occ", picker);

    //fetching lots
    let data;
    try{
      const res = await fetch(`/api/lots/${campus}`);
      data = await res.json();
      if(!data.lots || !Array.isArray(data.lots)) throw new Error("Bad payload");
    }catch(e){
      if(statusEl) statusEl.textContent = "Failed to load lots.";
      return;
    }

    //filling dropdown
    menu.innerHTML = data.lots
      .map(l => `<li role="option" data-name="${l.name.replace(/"/g,'&quot;')}">${l.name}</li>`)
      .join("");

    function setLotByName(name){
      const entry = data.lots.find(l => l.name === name) || data.lots[0];
      lotNameEl.textContent = entry.name;

      //donut chart
      renderDonut(picker, entry.total, entry.occupied);

      //per-lot time series & metrics
      const stats = generateLotStats(entry.name);
      renderOccupancyChart(picker, stats.points);
      if(avgDurEl) avgDurEl.textContent = stats.avgDurationLabel;
      if(avgOccEl) avgOccEl.textContent = `${stats.avgOcc}%`;

      const acts = generateRecentActivity(entry.name);
      renderActivityTable(picker, acts);

      //marking selected
      [...menu.children].forEach(li => {
        li.setAttribute("aria-selected", li.dataset.name === entry.name ? "true" : "false");
      });
    }

    function toggle(open){
      const isOpen = menu.style.display === "block";
      const next = (open === undefined) ? !isOpen : !!open;
      menu.style.display = next ? "block" : "none";
      title.setAttribute("aria-expanded", String(next));
    }

    title.addEventListener("click", ()=> toggle());
    title.addEventListener("keydown", (e)=>{
      if(e.key === "Enter" || e.key === " "){
        e.preventDefault();
        toggle();
      }
    });

    menu.addEventListener("click", (e)=>{
      const li = e.target.closest("li");
      if(!li) return;
      setLotByName(li.dataset.name);
      toggle(false);
    });

    window.addEventListener("click", (e)=>{
      if(!title.contains(e.target) && !menu.contains(e.target)) toggle(false);
    });

    //initial lot
    setLotByName(data.lots[0].name);
  }

  /*
  //log form
  function initLogForm(){
    const form = document.getElementById("logForm");
    const status = document.getElementById("logStatus");
    if(!form) return;

    form.addEventListener("submit", async (e)=>{
      e.preventDefault();
      if(status) status.textContent = "Submitting…";
      const payload = Object.fromEntries(new FormData(form).entries());
      try{
        const res = await fetch("/api/log", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(payload)
        });
        const json = await res.json();
        if(status) status.textContent = json.ok ? "Logged!" : (json.error || "Error logging.");
      }catch(err){
        if(status) status.textContent = "Network error";
      }
    });
  }*/

  //"DD/MM/YY"
function fmtDate(d){
  const dd = String(d.getDate()).padStart(2,'0');
  const mm = String(d.getMonth()+1).padStart(2,'0');
  const yy = String(d.getFullYear()).slice(-2);
  return `${dd}/${mm}/${yy}`;
}
//"HH:MM"
function fmtTime(d){
  const hh = String(d.getHours()).padStart(2,'0');
  const mi = String(d.getMinutes()).padStart(2,'0');
  return `${hh}:${mi}`;
}

//TO BE DELETED AFTER REAL DATA ->
//seeded list of recent events for a lot
function generateRecentActivity(lotName, count=6){
  const seed = hash32(lotName + "|events");
  const r = rng(seed);
  const now = new Date();
  const rows = [];
  let t = new Date(now);
  for(let i=0;i<count;i++){
    // step back between 1–15 minutes each row
    t = new Date(t.getTime() - (1 + Math.floor(r()*15)) * 60 * 1000);
    const event = r() < 0.65 ? "EXIT" : "ENTRY"; // exits are more common
    rows.push({ date: fmtDate(t), time: fmtTime(t), event });
  }
  // latest first
  return rows.sort((a,b)=> (a.date + a.time) < (b.date + b.time) ? 1 : -1);
}
// <- TO BE DELETED AFTER REAL DATA 

//injecting rows into the table
function renderActivityTable(root, rows){
  const tbody = root.querySelector("#activity-table tbody");
  if(!tbody) return;
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.date}</td>
      <td>${r.time}</td>
      <td>${r.event}</td>
    </tr>
  `).join("");
}


  //setting up the entire dashboard and logic
  addEventListener("DOMContentLoaded", ()=>{
    initCampus();
    //initLogForm();
  });
})();
