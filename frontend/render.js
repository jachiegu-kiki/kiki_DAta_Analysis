/* ============================================================
 * render.js — 視覺渲染層
 * ------------------------------------------------------------
 * 職責：
 *   1. KPI 卡片構造（pgHTML / kCard）
 *   2. 主渲染入口（renderAll）— 觸發所有子模塊重繪
 *   3. 子模塊渲染：
 *      ├─ renderPay         收款 KPI
 *      ├─ renderSign        净签 KPI（含進度條）
 *      ├─ renderRegionComparison  條線 / 板塊對照（雙維度大表）
 *      ├─ renderPotential   潛在簽約（部門展開 + 合同明細）
 *      └─ renderAdvisors    顧問雙榜
 *   4. 渲染狀態變量（nsAll / mmAll / expanded）+ 切換 handler
 *
 * 依賴 core.js：$、N、P、cls、clsI、bdg、hash、RAW、CU
 * 被 app.js 調用：renderAll
 * 被 HTML onclick 調用：tglAdv、rcSwitchTab、tglDept（內部觸發）
 * ============================================================ */

/* ═══ 渲染狀態 ═══ */
var nsAll = false, mmAll = false;
var expanded = new Set();

/* ═══ 進度條 + KPI 卡片構造 ═══ */
function pgHTML(val, target, timeP) {
  // v4.4 修復：篩選後查不到目標 → 顯示 0 軌道而非整條消失。
  //   target === undefined → 該 KPI 本身無目標概念（日/週），不畫；
  //   target === null / 0 / NaN → 顯示空軌道「目标 0万 / —」，表明「已查询但无数据」；
  //   target > 0 → 正常進度條。
  if (target === undefined) return '';
  var tp = Math.min(Math.max(timeP || 0, 0), 100);
  var hasTarget = (target != null) && !isNaN(target) && target > 0;
  if (!hasTarget) {
    return '<div class="pg"><div class="pg-hd"><span>目标 0万</span>'
      + '<span style="color:var(--t3);font-weight:600">—</span></div>'
      + '<div class="pg-track">'
      + '<div class="pg-marker" style="left:'+tp+'%" title="时间进度 '+N(tp)+'%"></div>'
      + '</div></div>';
  }
  var pct = Math.min(val / target * 100, 100);
  var bc = val / target >= 1 ? 'hit' : 'miss';
  var pc = val / target >= 1 ? 'var(--grn)' : 'var(--org)';
  return '<div class="pg"><div class="pg-hd"><span>目标 ' + N(target) + '万</span>'
    + '<span style="color:'+pc+';font-weight:600">' + N(val/target*100) + '%</span></div>'
    + '<div class="pg-track"><div class="pg-bar '+bc+'" style="width:'+pct+'%"></div>'
    + '<div class="pg-marker" style="left:'+tp+'%" title="时间进度 '+N(tp)+'%"></div>'
    + '</div></div>';
}

function kCard(lbl, tag, val, sub, bdgs, extra) {
  extra = extra || '';
  return '<div class="kc"><div class="kc-lbl">' + lbl + ' <span style="font-size:9px;color:var(--t3);font-family:var(--mono);font-weight:400">' + tag + '</span></div>'
    + '<div class="kc-val' + ((val||0) < 0 ? ' neg' : '') + '">' + N(val) + '<sup>万</sup></div>'
    + (sub ? '<div class="kc-sub">' + sub + '</div>' : '')
    + '<div class="kc-bd">' + bdgs.join('') + '</div>' + extra + '</div>';
}

/* ═══ 主渲染入口（不再有 scaleKpi / getMul — 后端已精确过滤）═══ */
function renderAll(resetExp) {
  if (resetExp === undefined) resetExp = true;
  if (!RAW) return;
  if (resetExp) expanded.clear();
  var h = RAW.header;
  $('hd-date').textContent = h.execution_date;
  $('hd-mth').textContent = N(h.monthly_time_progress) + '%';
  $('hd-fy').textContent = N(h.fiscal_time_progress) + '%';
  $('hd-wk').textContent = h.fiscal_week_start + ' (FW' + (h.fiscal_week_number||'') + ')';
  $('hd-ts').textContent = '更新 ' + h.update_time;
  if (CU) $('hd-user').textContent = CU.label + ' · 登出';
  // 手机端精简元数据
  if ($('hd-date-m')) $('hd-date-m').textContent = h.execution_date;
  if ($('hd-mth-m')) $('hd-mth-m').textContent = N(h.monthly_time_progress) + '%';
  if ($('hd-fy-m')) $('hd-fy-m').textContent = N(h.fiscal_time_progress) + '%';
  renderPay(); renderSign(); renderRegionComparison(); renderPotential(); renderAdvisors();
}

function renderPay() {
  var k = RAW.kpi_payment;
  $('kpi-pay').innerHTML =
    kCard('昨日收款','DAILY',k.daily.value,null,[bdg('日环比 '+P(k.daily.wow_pct),clsI(k.daily.wow_pct)),bdg('同比 '+P(k.daily.yoy_pct),clsI(k.daily.yoy_pct))])
    +kCard('本周收款','WEEKLY',k.weekly.value,null,[bdg('周环比 '+P(k.weekly.wow_pct),clsI(k.weekly.wow_pct)),bdg('同比 '+P(k.weekly.yoy_pct),clsI(k.weekly.yoy_pct))])
    +kCard('本月收款','MTD',k.monthly.value,null,[bdg('同比 '+P(k.monthly.yoy_pct),clsI(k.monthly.yoy_pct)),bdg('月环比 '+P(k.monthly.mom_pct),clsI(k.monthly.mom_pct))])
    +kCard('财年收款','FISCAL',k.fiscal_year.value,null,[bdg('同比 '+P(k.fiscal_year.yoy_pct),clsI(k.fiscal_year.yoy_pct))]);
}

function renderSign() {
  var k = RAW.kpi_signing, h = RAW.header;
  $('kpi-sign').innerHTML =
    kCard('昨日净签','DAILY',k.daily.value,'毛签 '+N(k.daily.gross_sign)+' / 退费 '+N(k.daily.refund),[bdg('日环比 '+P(k.daily.wow_pct),cls(k.daily.wow_pct)),bdg('同比 '+P(k.daily.yoy_pct),cls(k.daily.yoy_pct))])
    +kCard('本周净签','WEEKLY',k.weekly.value,'毛签 '+N(k.weekly.gross_sign)+' / 退费 '+N(k.weekly.refund),[bdg('周环比 '+P(k.weekly.wow_pct),cls(k.weekly.wow_pct)),k.weekly.yoy_abs!=null?bdg('同比'+(k.weekly.yoy_abs>=0?'+':'')+N(k.weekly.yoy_abs)+'万',k.weekly.yoy_abs>=0?'up':'dn'):''])
    +kCard('本月净签','MTD',k.monthly.value,'毛签 '+N(k.monthly.gross_sign)+' / 退费 '+N(k.monthly.refund),[bdg('同比 '+P(k.monthly.yoy_pct),cls(k.monthly.yoy_pct)),bdg('月环比 '+P(k.monthly.mom_pct),cls(k.monthly.mom_pct))],pgHTML(k.monthly.value,k.monthly.target,h.monthly_time_progress))
    +kCard('财年净签','FISCAL',k.fiscal_year.value,'毛签 '+N(k.fiscal_year.gross_sign)+' / 退费 '+N(k.fiscal_year.refund),[bdg('同比 '+P(k.fiscal_year.yoy_pct),cls(k.fiscal_year.yoy_pct)),bdg((k.fiscal_year.gap||0)<0?'已超额 '+N(Math.abs(k.fiscal_year.gap||0))+'万':'缺口 '+N(k.fiscal_year.gap||0)+'万',(k.fiscal_year.gap||0)<0?'up':'nt')],pgHTML(k.fiscal_year.value,k.fiscal_year.target,h.fiscal_time_progress));
}

function renderPotential() {
  var fw = RAW.potential;
  var depts = (fw.departments || []).filter(function(d) { return d.unarchived > 0 || d.unconfirmed > 0; });
  var totalUA = 0, totalUC = 0;
  depts.forEach(function(d) { totalUA += d.unarchived || 0; totalUC += d.unconfirmed || 0; });
  var netSigned = RAW.kpi_signing.monthly.value || 0;
  var netWithP = netSigned + totalUA + totalUC;

  $('pc-row').innerHTML = '<div class="pc arch"><div class="pc-lbl">已收款未盖章 合计</div><div class="pc-val">' + N(totalUA) + ' 万</div></div>'
    + '<div class="pc unrec"><div class="pc-lbl">未认款 合计</div><div class="pc-val">' + N(totalUC) + ' 万</div></div>'
    + '<div class="pc net"><div class="pc-lbl">净签（含潜在）合计</div><div class="pc-val">' + N(netWithP) + ' 万</div>'
    + '<div class="pc-formula">净签 ' + N(netSigned) + ' ＋ 未盖章 ' + N(totalUA) + ' ＋ 未认款 ' + N(totalUC) + '</div></div>';

  var rows = '';
  depts.forEach(function(d) {
    var isOpen = expanded.has(d.name), hd = hash(d.name);
    var hasCt = d.contracts && d.contracts.length > 0;
    var oc = hasCt ? 'tglDept(&apos;' + d.name.replace(/'/g,'&apos;') + '&apos;)' : '';
    var icon = hasCt ? '<span class="ex-icon' + (isOpen?' open':'') + '" id="ei'+hd+'">▶</span>' : '<span style="width:14px;display:inline-block"></span>';
    var badge = hasCt ? '<span class="dt has">' + d.contracts.length + '项</span>' : '';
    var toggle = hasCt ? ' <span style="font-size:10px;color:var(--t3)">' + (isOpen ? '▲' : '▼') + '</span>' : '';
    rows += '<tr class="dr" onclick="' + oc + '"><td>' + icon + d.name + badge + toggle + '</td>'
      + '<td></td>'
      + '<td class="am ' + (d.unarchived>0?'wo':'z') + '">' + (d.unarchived>0?N(d.unarchived):'—') + '</td>'
      + '<td class="am ' + (d.unconfirmed>0?'wg':'z') + '">' + (d.unconfirmed>0?N(d.unconfirmed):'—') + '</td></tr>';
    if (hasCt) { d.contracts.forEach(function(c) {
      rows += '<tr class="cr' + (isOpen?'':' hidden') + '"><td>'
        + '<span style="color:var(--t0);font-weight:500">' + (c.student||c.contract_no||'') + '</span>'
        + '<span class="ct-no"> (' + (c.contract_no||'') + ')</span></td>'
        + '<td class="ct-adv am" style="text-align:right">' + (c.advisor||'') + '</td>'
        + '<td class="am ' + ((c.unarchived||0)>0?'wo':'z') + '" style="font-size:12px">' + ((c.unarchived||0)>0?N(c.unarchived):'—') + '</td>'
        + '<td class="am ' + ((c.unconfirmed||0)>0?'wg':'z') + '" style="font-size:12px">' + ((c.unconfirmed||0)>0?N(c.unconfirmed):'—') + '</td></tr>';
    }); }
  });
  $('pt-body').innerHTML = rows || '<tr><td colspan="4" style="text-align:center;color:var(--t3);padding:20px">无匹配数据</td></tr>';
  $('pt-foot').innerHTML = '<tr><td>合 计</td><td></td><td class="am">' + N(totalUA) + '</td><td class="am">' + N(totalUC) + '</td></tr>';
}

function tglDept(name) { expanded.has(name) ? expanded.delete(name) : expanded.add(name); renderPotential(); }

function renderAdvisors() {
  if (!RAW) return;
  var ns = (RAW.advisor_net_sign || []).map(function(a,i) { return Object.assign({}, a, {rank:i+1}); });
  var mm = (RAW.advisor_million || []).map(function(a,i) { return Object.assign({}, a, {rank:i+1}); });
  $('ns-cnt').textContent = ns.length + ' 名';
  $('mm-cnt').textContent = mm.length + ' 名';
  var vis = function(arr, all) { return all ? arr : arr.slice(0, 10); };

  $('ns-body').innerHTML = vis(ns, nsAll).map(function(r) {
    var rc = r.rank===1?'g1':r.rank===2?'g2':r.rank===3?'g3':'';
    return '<tr><td class="rn '+rc+'">'+r.rank+'</td>'
      + '<td>'+r.name+((r.multilang||0)>0?'<span class="ml-tag">多语</span>':'')+'</td>'
      + '<td class="mn '+((r.net_sign||0)<0?'nv':'')+'" style="text-align:right">'+N(r.net_sign)+'</td>'
      + '<td class="mn" style="text-align:right">'+N(r.gross_sign)+'</td>'
      + '<td class="mn '+((r.refund||0)>0?'nv':'')+'" style="text-align:right">'+((r.refund||0)>0?N(r.refund):'—')+'</td></tr>';
  }).join('') || '<tr><td colspan="5" style="text-align:center;color:var(--t3);padding:20px">无匹配数据</td></tr>';

  $('mm-body').innerHTML = vis(mm, mmAll).map(function(r) {
    var rc = r.rank===1?'g1':r.rank===2?'g2':r.rank===3?'g3':'';
    return '<tr><td class="rn '+rc+'">'+r.rank+'</td><td>'+r.name+'</td>'
      + '<td class="mn" style="text-align:right">'+N(r.total_payment)+'</td>'
      + '<td class="mn" style="text-align:right">'+N(r.gross_sign)+'</td>'
      + '<td class="mn '+((r.unarchived_unconfirmed||0)>0?'nv':'')+'" style="text-align:right">'
      + ((r.unarchived_unconfirmed||0)>0?N(r.unarchived_unconfirmed):'—')+'</td></tr>';
  }).join('') || '<tr><td colspan="5" style="text-align:center;color:var(--t3);padding:20px">无匹配数据</td></tr>';

  var setBtnState = function(btn, txt, arr, all) {
    if (arr.length <= 10) { btn.style.display = 'none'; return; }
    btn.style.display = 'flex';
    $(txt).textContent = all ? '↑ 收起（仅显示10/'+arr.length+'）' : '↓ 展开全部 ('+arr.length+')';
  };
  setBtnState($('ns-btn'),'ns-txt', ns, nsAll);
  setBtnState($('mm-btn'),'mm-txt', mm, mmAll);
}

function tglAdv(w) { if (w==='ns') nsAll=!nsAll; else mmAll=!mmAll; renderAdvisors(); }

/* ============================================================
 * 条线 / 板块 对照看板 (REGION COMPARISON)
 * ------------------------------------------------------------
 * 期望后端在 daily-report 接口的 RAW 中追加字段 region_comparison：
 *
 *   RAW.region_comparison = {
 *     line: {                                  // 按合同所属条线归集
 *       daily:   [ { name, gross, refund, net }, x4 行 ],
 *       weekly:  [ ..., x4 行 ],
 *       monthly: [ ..., x4 行 ],
 *       fiscal:  [ ..., x4 行 ]
 *     },
 *     team: {                                  // 按签约顾问所属团队归集
 *       daily:   [ ..., x4 行 ],
 *       weekly:  [ ..., x4 行 ],
 *       monthly: [ ..., x4 行 ],
 *       fiscal:  [ ..., x4 行 ]
 *     }
 *   }
 *
 * 每个数组固定 4 行，name ∈ { '大北美', '英国', '澳新', '欧亚' }
 * gross/refund/net 单位均为「万」，refund 为正数表示退费金额
 * 权限控制：后端按当前用户角色仅返回其可见行（如英国权限用户只返回英国 1 行）
 * ============================================================ */

// 当前选中 Tab：'dw' = 日&周, 'mfy' = 月&财年
var RC_TAB = 'mfy';

// 兜底 mock 数据：在后端尚未实装 region_comparison 字段时使用
// 接口对接后可删除该常量及 || RC_MOCK_DATA 兜底
var RC_MOCK_DATA = {
  line: {
    daily:   [{name:'大北美',gross:16.35,refund:6.61,net:9.74},  {name:'英国',gross:7.59,refund:1.48,net:6.11}, {name:'澳新',gross:9.34,refund:0.64,net:8.70}, {name:'欧亚',gross:15.12,refund:1.32,net:13.80}],
    weekly:  [{name:'大北美',gross:81.77,refund:33.03,net:48.74},{name:'英国',gross:37.97,refund:7.40,net:30.57},{name:'澳新',gross:46.69,refund:3.18,net:43.51},{name:'欧亚',gross:75.59,refund:6.62,net:68.97}],
    monthly: [{name:'大北美',gross:327.09,refund:132.13,net:194.96},{name:'英国',gross:151.89,refund:29.60,net:122.29},{name:'澳新',gross:186.76,refund:12.73,net:174.03},{name:'欧亚',gross:302.37,refund:26.46,net:275.91}],
    fiscal:  [{name:'大北美',gross:1144.82,refund:462.46,net:682.36},{name:'英国',gross:531.62,refund:103.60,net:428.02},{name:'澳新',gross:653.66,refund:44.56,net:609.10},{name:'欧亚',gross:1058.30,refund:92.61,net:965.69}]
  },
  team: {
    daily:   [{name:'大北美',gross:15.80,refund:6.95,net:8.85}, {name:'英国',gross:7.85,refund:1.50,net:6.35}, {name:'澳新',gross:9.10,refund:0.75,net:8.35}, {name:'欧亚',gross:15.65,refund:0.85,net:14.80}],
    weekly:  [{name:'大北美',gross:79.20,refund:35.10,net:44.10},{name:'英国',gross:38.50,refund:7.45,net:31.05},{name:'澳新',gross:45.80,refund:3.30,net:42.50},{name:'欧亚',gross:78.52,refund:4.38,net:74.14}],
    monthly: [{name:'大北美',gross:320.50,refund:138.20,net:182.30},{name:'英国',gross:155.20,refund:30.10,net:125.10},{name:'澳新',gross:178.30,refund:11.90,net:166.40},{name:'欧亚',gross:314.11,refund:20.72,net:293.39}],
    fiscal:  [{name:'大北美',gross:1115.70,refund:485.80,net:629.90},{name:'英国',gross:540.80,refund:106.20,net:434.60},{name:'澳新',gross:631.20,refund:41.32,net:589.88},{name:'欧亚',gross:1100.69,refund:69.90,net:1030.79}]
  }
};

// 时段 key → 中文标签
var RC_PERIOD_LABEL = { daily:'日度', weekly:'周度', monthly:'月度', fiscal:'财年度' };
// Tab → 两个时段 key
var RC_SET = { dw:['daily','weekly'], mfy:['monthly','fiscal'] };

// 数字格式化：保留 2 位小数
function rcFmt(n) { return (Number(n) || 0).toFixed(2); }

// 计算合计行：[gross_sum, refund_sum, net_sum]
function rcSumRows(rows) {
  var s = [0, 0, 0];
  (rows || []).forEach(function(r) { s[0] += (+r.gross||0); s[1] += (+r.refund||0); s[2] += (+r.net||0); });
  return s;
}

// 构造一张大表 HTML（条线 or 板块，含两个时段合并表头）
function rcBuildCard(view, p1, p2, data) {
  var isLine = (view === 'line');
  var badge  = isLine
    ? '<span class="rc-badge line">LINE</span>'
    : '<span class="rc-badge team">TEAM</span>';
  var name   = isLine ? '条线维度' : '业务板块维度';
  var sub    = isLine ? '按合同所属条线归集' : '按签约顾问所属团队归集';

  var r1 = (data[view] && data[view][p1]) || [];
  var r2 = (data[view] && data[view][p2]) || [];
  var s1 = rcSumRows(r1);
  var s2 = rcSumRows(r2);

  // 按 4 个固定分类对齐：r1[i] 与 r2[i] 应同名（后端保证顺序一致）
  // 若后端权限过滤导致某行缺失，按前端兜底逻辑用占位
  var maxLen = Math.max(r1.length, r2.length, 0);
  var bodyHtml = '';
  for (var i = 0; i < maxLen; i++) {
    var a = r1[i] || { name: (r2[i]||{}).name || '', gross:0, refund:0, net:0 };
    var b = r2[i] || { name: (r1[i]||{}).name || '', gross:0, refund:0, net:0 };
	
	if (a.gross == 0 && a.refund == 0 && a.net == 0 &&
        b.gross == 0 && b.refund == 0 && b.net == 0) {
      continue;
    }
	
    bodyHtml += '<tr>'
      + '<td class="cat">' + a.name + '</td>'
      + '<td>' + rcFmt(a.gross)  + '</td>'
      + '<td class="ref">' + rcFmt(a.refund) + '</td>'
      + '<td class="net">' + rcFmt(a.net)    + '</td>'
      + '<td class="start">' + rcFmt(b.gross)  + '</td>'
      + '<td class="ref">'   + rcFmt(b.refund) + '</td>'
      + '<td class="net">'   + rcFmt(b.net)    + '</td>'
      + '</tr>';
  }

  return '<div class="rc-card">'
    + '<div class="rc-card-hd">' + badge + '<span class="rc-card-name">' + name + '</span><span class="rc-card-sub">' + sub + '</span></div>'
    + '<table class="rc-tbl">'
    +   '<colgroup><col class="cat"><col><col><col><col><col><col></colgroup>'
    +   '<thead>'
    +     '<tr>'
    +       '<td class="col-cat" rowspan="2">分类</td>'
    +       '<td class="grp" colspan="3">' + RC_PERIOD_LABEL[p1] + '</td>'
    +       '<td class="grp g2" colspan="3">' + RC_PERIOD_LABEL[p2] + '</td>'
    +     '</tr>'
    +     '<tr>'
    +       '<th class="sub-th">毛签</th><th class="sub-th">退费</th><th class="sub-th">净签</th>'
    +       '<th class="sub-th start">毛签</th><th class="sub-th">退费</th><th class="sub-th">净签</th>'
    +     '</tr>'
    +   '</thead>'
    +   '<tbody>' + bodyHtml + '</tbody>'
    +   '<tfoot>'
    +     '<tr>'
    +       '<td class="cat">合 计</td>'
    +       '<td>' + rcFmt(s1[0]) + '</td><td><span class="ref">' + rcFmt(s1[1]) + '</span></td><td><span class="net">' + rcFmt(s1[2]) + '</span></td>'
    +       '<td class="start">' + rcFmt(s2[0]) + '</td><td><span class="ref">' + rcFmt(s2[1]) + '</span></td><td><span class="net">' + rcFmt(s2[2]) + '</span></td>'
    +     '</tr>'
    +   '</tfoot>'
    + '</table>'
    + '</div>';
}

// 主渲染函数：按当前 Tab 渲染左右两张大表
function renderRegionComparison() {
  var pair = $('rc-pair');
  if (!pair) return;
  if (!RAW) return;

  var data = RAW.region_comparison || RC_MOCK_DATA;
  var periods = RC_SET[RC_TAB] || RC_SET.mfy;

  pair.innerHTML =
      rcBuildCard('line', periods[0], periods[1], data)
    + rcBuildCard('team', periods[0], periods[1], data);
}

// Tab 切换 handler（onclick 直接调用）
function rcSwitchTab(setKey) {
  if (!RC_SET[setKey]) return;
  RC_TAB = setKey;
  var tabs = document.querySelectorAll('#rc-tabs .rc-tab');
  for (var i = 0; i < tabs.length; i++) {
    if (tabs[i].getAttribute('data-set') === setKey) tabs[i].classList.add('on');
    else tabs[i].classList.remove('on');
  }
  renderRegionComparison();
}
