var $ = function(id){return document.getElementById(id)};
var API = window.location.origin;
var RAW = null;
var CU = null;
var origExecDate = null;
// 后端返回的级联筛选元数据
var ADL = {};         // advisor_dept_links（兼容）
var ALL_DEPTS = [];   // 兼容旧版
var ALL_ADVS = [];    // 全量顾问列表
var FO = {};          // filter_options: {lines, sub_lines, group_sys, biz_blocks, group_l1, group_advisor}

/* ═══ AUTH (v2: Gateway Cookie-based SSO) ═══
   不再自己做登入；未登入時 Gateway 回 401，跳 /auth/login。
*/
function gotoLogin() { window.location.href = '/auth/login'; }
function doLogout()  { window.location.href = '/auth/logout'; }

async function loadMe() {
  try {
    var res = await fetch(API + '/api/v1/me', { credentials: 'same-origin' });
    if (res.status === 401) { gotoLogin(); return null; }
    if (!res.ok) return null;
    var d = await res.json();
    return {
      role: d.role,                           // finance 层 role
      gw_role: d.gw_role || '',               // v4: gateway 层 role
      is_system_admin: !!d.is_system_admin,   // v4: 后端已算好
      username: d.username,
      dept_scope: d.department_scope,
      advisor_name: d.advisor_name,
      label: (d.display_name || d.username) + ' · ' +
             ({ADMIN:'总盘管理员', MANAGER:'部门经理', ADVISOR:'顾问'}[d.role] || d.role)
    };
  } catch(e) { console.error('loadMe error', e); return null; }
}

/* ═══ FETCH — 核心：支持多维度筛选 ═══ */
async function fetchReport(execDate, filterParams) {
  var url = API + '/api/v1/dashboard/daily-report';
  var qs = [];
  if (execDate) qs.push('execution_date=' + execDate);
  var fp = filterParams || {};
  if (fp.advisors && fp.advisors.length) qs.push('advisors=' + encodeURIComponent(fp.advisors.join(',')));
  if (fp.filter_line && fp.filter_line.length) qs.push('filter_line=' + encodeURIComponent(fp.filter_line.join(',')));
  if (fp.filter_sub_line && fp.filter_sub_line.length) qs.push('filter_sub_line=' + encodeURIComponent(fp.filter_sub_line.join(',')));
  if (fp.filter_group_sys && fp.filter_group_sys.length) qs.push('filter_group_sys=' + encodeURIComponent(fp.filter_group_sys.join(',')));
  if (fp.filter_biz_block && fp.filter_biz_block.length) qs.push('filter_biz_block=' + encodeURIComponent(fp.filter_biz_block.join(',')));
  if (fp.filter_group_l1 && fp.filter_group_l1.length) qs.push('filter_group_l1=' + encodeURIComponent(fp.filter_group_l1.join(',')));
  if (fp.filter_group_advisor && fp.filter_group_advisor.length) qs.push('filter_group_advisor=' + encodeURIComponent(fp.filter_group_advisor.join(',')));
  if (fp.filter_biz_type && fp.filter_biz_type.length) qs.push('filter_biz_type=' + encodeURIComponent(fp.filter_biz_type.join(',')));
  if (qs.length) url += '?' + qs.join('&');

  try {
    var res = await fetch(url, { credentials: 'same-origin' });
    if (res.status === 401) { gotoLogin(); return; }
    var data = await res.json();
    data.potential = data.potential || data.fund_warning || {total_unarchived:0, total_unconfirmed:0, departments:[]};
    (data.potential.departments || []).forEach(function(d) { if (!d.contracts) d.contracts = []; });

    RAW = data;

    // 首次加载：保存元数据
    if (!origExecDate) {
      origExecDate = data.header.execution_date;
      ADL = data.advisor_dept_links || {};
      ALL_DEPTS = data.all_depts || [];
      ALL_ADVS = data.all_advisors || [];
      FO = data.filter_options || {};
      buildOptions();
    }

    $('dash').style.display = 'block';
    $('demo-text').textContent = '数据来源：FastAPI + PostgreSQL · 报表日期 ' + data.header.execution_date;
    renderAll();
  } catch(e) { console.error('fetch error:', e); }
}

// 啟動：先拿當前用戶，再載入報表
(async function init() {
  CU = await loadMe();
  if (!CU) return;  // 401 已被 loadMe 跳轉處理
  // 角色網關：ADVISOR → body 加 class，CSS 自動隱藏潛在簽約 / 顧問排行 / 鎖顧問篩選器
  if (CU.role === 'ADVISOR') {
    document.body.classList.add('role-advisor');
  }
  // v4: 非系统管理员（gw_role != 'admin'）隐藏『同步数据』按钮
  // 后端 /api/v1/etl/trigger 仍独立做 require_system_admin 防御（defense-in-depth）
  if (!CU.is_system_admin) {
    var etlBtn = document.getElementById('btn-etl');
    if (etlBtn) etlBtn.style.display = 'none';
  }
  // v5.1 手机端 UX：将筛选器重组为手风琴结构（桌面端不生效）
  setupMobileAccordion();
  await fetchReport();
})();

/* ═══ v5.1 手机端手风琴筛选器
   第一性原理：原 HTML 13 个筛选器扁平排列，手机端 1 栏会占 13 行高，
   严重超过 Miller's Law 认知负荷（7±2）。按照现有的 3 个分组标签
   （按条线/按团队/通用）做手风琴折叠，默认全部收起。
   做法：纯 DOM 重组，不侵入 HTML 结构、不影响桌面端渲染、不改 JS 逻辑。
   ─────────────────────────────────────────────────────────── */
function setupMobileAccordion() {
  var inner = document.querySelector('.fp-wrap .fp-inner');
  if (!inner) return;
  // 识别分组标签（通过 fp-lbl 里的文字特征 ▎开头）
  var children = Array.from(inner.children);
  var groups = [];  // [{head: groupEl, items: [groupEl,...]}]
  var current = null;
  children.forEach(function(el) {
    if (!el.classList.contains('fp-group')) return;
    var lbl = el.querySelector('.fp-lbl');
    if (lbl && /^▎/.test((lbl.textContent || '').trim())) {
      // 新分组标签
      if (current) groups.push(current);
      current = { head: el, items: [] };
    } else if (current) {
      current.items.push(el);
    }
  });
  if (current) groups.push(current);
  if (groups.length === 0) return;  // 无分组标签 → 保持原样

  groups.forEach(function(g, idx) {
    // 1. 分组 head 挂 class，加箭头和 badge
    g.head.classList.add('acc-head');
    g.head.setAttribute('data-acc-idx', idx);
    var headLbl = g.head.querySelector('.fp-lbl');
    if (headLbl && !headLbl.querySelector('.acc-badge')) {
      headLbl.insertAdjacentHTML('beforeend',
        ' <span class="acc-badge" data-acc-badge="'+idx+'">0</span>');
    }
    if (!g.head.querySelector('.acc-arrow')) {
      g.head.insertAdjacentHTML('beforeend', '<span class="acc-arrow">▾</span>');
    }
    // 2. 包裹 items 到 .fp-acc-body
    var body = document.createElement('div');
    body.className = 'fp-acc-body';
    body.setAttribute('data-acc-body', idx);
    g.items.forEach(function(it) { body.appendChild(it); });
    g.head.parentNode.insertBefore(body, g.head.nextSibling);
    // 3. head 点击切换展开
    g.head.addEventListener('click', function(e) {
      // 避免冒泡到内层 dd（虽然此时 body 已经不在 head 内部，但保险）
      if (e.target.closest('.dd')) return;
      this.classList.toggle('open');
      var b = document.querySelector('[data-acc-body="'+idx+'"]');
      if (b) b.classList.toggle('open');
    });
  });
}

/* 更新手风琴 head 上的 badge 数字（由已选筛选计数变化时调用）*/
function updateAccordionBadges() {
  document.querySelectorAll('[data-acc-body]').forEach(function(body) {
    var idx = body.getAttribute('data-acc-body');
    var count = body.querySelectorAll('.dd .dd-item input:checked').length;
    var badge = document.querySelector('[data-acc-badge="'+idx+'"]');
    var head  = document.querySelector('.acc-head[data-acc-idx="'+idx+'"]');
    if (badge) badge.textContent = count;
    if (head) {
      if (count > 0) head.classList.add('has-sel');
      else head.classList.remove('has-sel');
    }
  });
}

/* ═══ FILTER STATE ═══ */
var fLine=[], fSubLine=[], fGSys=[], fBlock=[], fGL1=[], fGAdv=[], fBType=[], fA=[], fFY=[], fYM=[], fFW=[];
var nsAll = false, mmAll = false;
var expanded = new Set();

/* ═══ Custom Dropdown Engine ═══ */
function ddToggle(id) {
  var el = $(id);
  var wasOpen = el.classList.contains('open');
  // close all
  document.querySelectorAll('.dd.open').forEach(function(d) { d.classList.remove('open'); });
  if (!wasOpen) el.classList.add('open');
}
document.addEventListener('click', function(e) {
  if (!e.target.closest('.dd')) {
    document.querySelectorAll('.dd.open').forEach(function(d) { d.classList.remove('open'); });
  }
});

function ddSetItems(id, items, opts) {
  opts = opts || {};
  var list = $(id).querySelector('.dd-list');
  var searchHtml = opts.searchable
    ? '<div class="dd-search-wrap"><input class="dd-search" type="text" placeholder="输入搜索..." oninput="ddFilter(\'' + id + '\',this.value)"></div>'
    : '';
  var itemsHtml = items.map(function(v) {
    return '<label class="dd-item" data-val="' + v + '"><input type="checkbox" value="' + v + '" onchange="ddOnChange(\'' + id + '\')"><span>' + v + '</span></label>';
  }).join('');
  list.innerHTML = searchHtml + itemsHtml;
  ddUpdateBtn(id);
}

function ddFilter(id, q) {
  q = (q || '').trim().toLowerCase();
  var any = false;
  $(id).querySelectorAll('.dd-item').forEach(function(item) {
    var v = (item.getAttribute('data-val') || '').toLowerCase();
    var show = !q || v.indexOf(q) >= 0;
    item.style.display = show ? '' : 'none';
    if (show) any = true;
  });
  var list = $(id).querySelector('.dd-list');
  var empty = list.querySelector('.dd-empty');
  if (!any && !empty) {
    var e = document.createElement('div');
    e.className = 'dd-empty';
    e.textContent = '无匹配结果';
    list.appendChild(e);
  } else if (any && empty) {
    empty.remove();
  }
}

function ddGetSelected(id) {
  var checks = $(id).querySelectorAll('.dd-item input:checked');
  return Array.from(checks).map(function(c) { return c.value; });
}

function ddClearAll(id) {
  $(id).querySelectorAll('.dd-item input').forEach(function(c) { c.checked = false; c.parentElement.classList.remove('checked'); });
  ddUpdateBtn(id);
}

function ddUpdateBtn(id) {
  var sel = ddGetSelected(id);
  var btn = $(id).querySelector('.dd-btn');
  if (sel.length === 0) {
    btn.innerHTML = '全部 <span class="dd-arrow">▾</span>';
    btn.classList.remove('has-sel');
  } else if (sel.length === 1) {
    btn.innerHTML = '<span style="max-width:100px;overflow:hidden;text-overflow:ellipsis">' + sel[0] + '</span> <span class="dd-arrow">▾</span>';
    btn.classList.add('has-sel');
  } else {
    btn.innerHTML = sel.length + '项已选 <span class="dd-arrow">▾</span>';
    btn.classList.add('has-sel');
  }
  // update checked class
  $(id).querySelectorAll('.dd-item').forEach(function(item) {
    var cb = item.querySelector('input');
    if (cb.checked) item.classList.add('checked'); else item.classList.remove('checked');
  });
}

function ddOnChange(id) {
  ddUpdateBtn(id);
  // v4.3 修复：级联只能向下，下游触发时不能重建自己（否则刚勾的 checkbox 会被 innerHTML 清掉）
  if (id === 'f-line')    { rebuildSubLine(); rebuildGsys(); }
  if (id === 'f-subline') { rebuildGsys(); }
  if (id === 'f-block')   { rebuildGl1(); rebuildGadv(); }
  if (id === 'f-gl1')     { rebuildGadv(); }
  // v5.1 手机端：刷新手风琴 head 徽章
  if (typeof updateAccordionBadges === 'function') updateAccordionBadges();
}

function opts(arr) { return arr; }
function optsObj(arr) { return arr.map(function(o){ return o.value; }); }

/* ═══ 构建筛选器选项 ═══ */
function buildOptions() {
  if (!RAW) return;
  var ed = RAW.header.execution_date || new Date().toISOString().slice(0,10);
  var curY = parseInt(ed.slice(0,4)), curM = parseInt(ed.slice(5,7));

  // 按条线筛选
  ddSetItems('f-line', FO.lines || []);
  ddSetItems('f-subline', optsObj(FO.sub_lines || []));
  ddSetItems('f-gsys', optsObj(FO.group_sys || []));

  // 按团队筛选
  ddSetItems('f-block', FO.biz_blocks || []);
  ddSetItems('f-gl1', optsObj(FO.group_l1 || []));
  ddSetItems('f-gadv', optsObj(FO.group_advisor || []));

  // 业务类型
  ddSetItems('f-btype', FO.biz_types || ['留学', '多语']);

  // 顾问（支持打字搜索；ADVISOR 角色後端已只回自己一人，前端做 visual lock）
  if (CU && CU.role === 'ADVISOR') {
    ddSetItems('f-adv', ALL_ADVS);
    // 預勾選自己，按鈕顯示名字而非「全部」
    var advList = $('f-adv').querySelector('.dd-list');
    if (advList) {
      advList.querySelectorAll('input[type=checkbox]').forEach(function(cb) {
        if (cb.value === CU.advisor_name) cb.checked = true;
      });
      ddUpdateBtn('f-adv');
    }
  } else {
    ddSetItems('f-adv', ALL_ADVS, {searchable: true});
  }

  // 财年
  var fyYear = curM >= 6 ? curY + 1 : curY;
  var fyArr = [];
  for (var y = fyYear; y >= fyYear - 3; y--) {
    fyArr.push('FY' + y + (y === fyYear ? '（当前）' : ''));
  }
  ddSetItems('f-fy', fyArr);

  // 年月
  var ymArr = [];
  for (var yy = curY; yy >= curY - 1; yy--) {
    var maxM = yy === curY ? curM : 12;
    for (var mm = maxM; mm >= 1; mm--) {
      var v = yy + '' + (mm < 10 ? '0' : '') + mm;
      ymArr.push(v);
    }
  }
  ddSetItems('f-ym', ymArr);

  // 财周
  var fwNum = RAW.header.fiscal_week_number;
  if (!fwNum) {
    var _d = new Date(ed), _fy = curM >= 6 ? new Date(curY,5,1) : new Date(curY-1,5,1);
    var _dow = _fy.getDay(), _dts = _dow === 0 ? 0 : 7 - _dow;
    var _fwe = new Date(_fy); _fwe.setDate(_fwe.getDate() + _dts);
    fwNum = _d <= _fwe ? 1 : 2 + Math.floor((_d - _fwe - 86400000) / 604800000);
  }
  var fwArr = [];
  for (var w = fwNum; w >= 1; w--) {
    fwArr.push('FW' + fyYear + '-' + w + (w === fwNum ? '（本周）' : ''));
  }
  ddSetItems('f-fw', fwArr);

  // v5.1 手机端：首次/重建选项后刷新手风琴 badge
  if (typeof updateAccordionBadges === 'function') updateAccordionBadges();
}

/* ═══ 级联联动（v4.3：单向重建，避免重建自身清掉选中状态）═══ */
function rebuildSubLine() {
  // 当 f-line 变化时调用：按选中的条线过滤出可见的二级条线
  var selLines = ddGetSelected('f-line');
  var slAll = FO.sub_lines || [];
  if (selLines.length > 0) {
    var lineSet = new Set(selLines);
    ddSetItems('f-subline', slAll.filter(function(o){ return lineSet.has(o.parent); }).map(function(o){return o.value;}));
  } else {
    ddSetItems('f-subline', slAll.map(function(o){return o.value;}));
  }
}

function rebuildGsys() {
  // 当 f-line 或 f-subline 变化时调用：按当前选中联动 f-gsys 可选项
  var selLines = ddGetSelected('f-line');
  var selSL = ddGetSelected('f-subline');
  var slAll = FO.sub_lines || [];
  var gsAll = FO.group_sys || [];
  if (selSL.length > 0) {
    var slSet = new Set(selSL);
    ddSetItems('f-gsys', gsAll.filter(function(o){ return slSet.has(o.parent); }).map(function(o){return o.value;}));
  } else if (selLines.length > 0) {
    var validSL = new Set(slAll.filter(function(o){ return new Set(selLines).has(o.parent); }).map(function(o){return o.value;}));
    ddSetItems('f-gsys', gsAll.filter(function(o){ return validSL.has(o.parent); }).map(function(o){return o.value;}));
  } else {
    ddSetItems('f-gsys', gsAll.map(function(o){return o.value;}));
  }
}

// 保留旧名以兼容外部调用（如初始化）: 完整向下级联
function cascadeLine() { rebuildSubLine(); rebuildGsys(); }

function rebuildGl1() {
  // 当 f-block 变化时调用：按选中的板块过滤出可见的一级组
  var selBlocks = ddGetSelected('f-block');
  var l1All = FO.group_l1 || [];
  if (selBlocks.length > 0) {
    var bSet = new Set(selBlocks);
    ddSetItems('f-gl1', l1All.filter(function(o){ return bSet.has(o.parent); }).map(function(o){return o.value;}));
  } else {
    ddSetItems('f-gl1', l1All.map(function(o){return o.value;}));
  }
}

function rebuildGadv() {
  // 当 f-block 或 f-gl1 变化时调用：按当前选中联动 f-gadv 可选项
  var selBlocks = ddGetSelected('f-block');
  var selL1 = ddGetSelected('f-gl1');
  var l1All = FO.group_l1 || [];
  var gaAll = FO.group_advisor || [];
  if (selL1.length > 0) {
    var l1Set = new Set(selL1);
    ddSetItems('f-gadv', gaAll.filter(function(o){ return l1Set.has(o.parent); }).map(function(o){return o.value;}));
  } else if (selBlocks.length > 0) {
    var validL1 = new Set(l1All.filter(function(o){ return new Set(selBlocks).has(o.parent); }).map(function(o){return o.value;}));
    ddSetItems('f-gadv', gaAll.filter(function(o){ return validL1.has(o.parent); }).map(function(o){return o.value;}));
  } else {
    ddSetItems('f-gadv', gaAll.map(function(o){return o.value;}));
  }
}

// 保留旧名以兼容外部调用: 完整向下级联
function cascadeTeam() { rebuildGl1(); rebuildGadv(); }

/* ═══ 时间筛选 → 计算 execution_date ═══ */
function computeExecDate() {
  if (fFW.length > 0) {
    var last = fFW.sort().pop();
    var parts = last.replace('FW','').split('-');
    var fy = parseInt(parts[0]), wk = parseInt(parts[1]);
    var fyStart = new Date(fy - 1, 5, 1);
    var dow = fyStart.getDay();
    var daysToSun = dow === 0 ? 0 : 7 - dow;
    var firstWeekEnd = new Date(fyStart); firstWeekEnd.setDate(firstWeekEnd.getDate() + daysToSun);
    if (wk <= 1) return fmtD(firstWeekEnd);
    var targetSun = new Date(firstWeekEnd);
    targetSun.setDate(targetSun.getDate() + (wk - 1) * 7);
    var fyEnd = new Date(fy, 4, 31);
    return fmtD(targetSun > fyEnd ? fyEnd : targetSun);
  }
  if (fYM.length > 0) {
    var latest = fYM.sort().pop();
    var y = parseInt(latest.slice(0,4)), m = parseInt(latest.slice(4));
    var now = origExecDate ? new Date(origExecDate) : new Date();
    if (y === now.getFullYear() && m === (now.getMonth()+1)) return origExecDate;
    var lastDay = new Date(y, m, 0).getDate();
    return y + '-' + (m < 10 ? '0' : '') + m + '-' + (lastDay < 10 ? '0' : '') + lastDay;
  }
  if (fFY.length > 0) {
    var latest2 = fFY.sort().pop();
    var fy2 = parseInt(latest2.replace('FY',''));
    var nowED = origExecDate || new Date().toISOString().slice(0,10);
    var nowFY = parseInt(nowED.slice(5,7)) >= 6 ? parseInt(nowED.slice(0,4))+1 : parseInt(nowED.slice(0,4));
    if (fy2 === nowFY) return origExecDate;
    return fy2 + '-05-31';
  }
  return null;
}
function fmtD(d) { return d.getFullYear() + '-' + (d.getMonth()+1 < 10 ? '0' : '') + (d.getMonth()+1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate(); }

/* ═══ APPLY / CLEAR ═══ */
async function applyFilters() {
  fLine = ddGetSelected('f-line'); fSubLine = ddGetSelected('f-subline'); fGSys = ddGetSelected('f-gsys');
  fBlock = ddGetSelected('f-block'); fGL1 = ddGetSelected('f-gl1'); fGAdv = ddGetSelected('f-gadv');
  fBType = ddGetSelected('f-btype');
  fA = ddGetSelected('f-adv');
  fFY = ddGetSelected('f-fy'); fYM = ddGetSelected('f-ym'); fFW = ddGetSelected('f-fw');
  // strip display suffixes for fy/fw values
  fFY = fFY.map(function(v){ return v.replace('（当前）',''); });
  fFW = fFW.map(function(v){ return v.replace('（本周）',''); });
  renderTags();
  $('ftags').innerHTML = '<span class="fp-loading">⏳ 正在加载数据...</span>';

  var targetDate = computeExecDate() || origExecDate;
  await fetchReport(targetDate, {
    advisors: fA,
    filter_line: fLine,
    filter_sub_line: fSubLine,
    filter_group_sys: fGSys,
    filter_biz_block: fBlock,
    filter_group_l1: fGL1,
    filter_group_advisor: fGAdv,
    filter_biz_type: fBType,
  });
  renderTags();
}

async function clearFilters() {
  ['f-line','f-subline','f-gsys','f-block','f-gl1','f-gadv','f-btype','f-adv','f-fy','f-ym','f-fw'].forEach(function(id) {
    ddClearAll(id);
  });
  fLine=[]; fSubLine=[]; fGSys=[]; fBlock=[]; fGL1=[]; fGAdv=[]; fBType=[]; fA=[]; fFY=[]; fYM=[]; fFW=[];
  buildOptions();
  renderTags();
  if (typeof updateAccordionBadges === 'function') updateAccordionBadges();
  $('ftags').innerHTML = '<span class="fp-loading">⏳ 正在加载数据...</span>';
  await fetchReport(origExecDate);
  renderTags();
}

function rmFilter(type, val) {
  var map = {ln:fLine, sl:fSubLine, gs:fGSys, bk:fBlock, l1:fGL1, ga:fGAdv, bt:fBType, a:fA, fy:fFY, ym:fYM, fw:fFW};
  map[type] = map[type].filter(function(v) { return v !== val; });
  if(type==='ln')fLine=map.ln; if(type==='sl')fSubLine=map.sl; if(type==='gs')fGSys=map.gs;
  if(type==='bk')fBlock=map.bk; if(type==='l1')fGL1=map.l1; if(type==='ga')fGAdv=map.ga;
  if(type==='bt')fBType=map.bt; if(type==='a')fA=map.a;
  if(type==='fy')fFY=map.fy; if(type==='ym')fYM=map.ym; if(type==='fw')fFW=map.fw;
  applyFilters();
}

function renderTags() {
  var tags = [];
  function mk(type, lbl, val) {
    return '<span class="fp-tag">' + lbl + ': <b>' + val + '</b>'
      + '<span class="x" onclick="rmFilter(&apos;' + type + '&apos;,&apos;' + val.replace(/'/g,'&apos;') + '&apos;)">×</span></span>';
  }
  fLine.forEach(function(v) { tags.push(mk('ln', '条线', v)); });
  fSubLine.forEach(function(v) { tags.push(mk('sl', '二级条线', v)); });
  fGSys.forEach(function(v) { tags.push(mk('gs', '分组(系统)', v)); });
  fBlock.forEach(function(v) { tags.push(mk('bk', '板块', v)); });
  fGL1.forEach(function(v) { tags.push(mk('l1', '一级分组', v)); });
  fGAdv.forEach(function(v) { tags.push(mk('ga', '分组(顾问)', v)); });
  fBType.forEach(function(v) { tags.push(mk('bt', '业务类型', v)); });
  fA.forEach(function(v) { tags.push(mk('a', '顾问', v)); });
  fFY.forEach(function(v) { tags.push(mk('fy', '财年', v)); });
  fYM.forEach(function(v) { tags.push(mk('ym', '年月', v.slice(0,4)+'年'+v.slice(4)+'月')); });
  fFW.forEach(function(v) { tags.push(mk('fw', '财周', v)); });
  $('ftags').innerHTML = tags.length ? tags.join('') : '<span class="fp-none">暂无筛选条件 · 显示当前权限下全量数据</span>';
}

/* ═══ UTILS ═══ */
function N(v, d) { d = d || 2; return v == null ? '—' : (+v).toFixed(d); }
function P(v) { return v == null ? '—' : (v >= 0 ? '+' : '') + N(v) + '%'; }
function cls(v) { return v == null ? 'nt' : v >= 0 ? 'up' : 'dn'; }
function clsI(v) { return v == null ? 'nt' : v >= 0 ? 'dn' : 'up'; }
function bdg(t, c) { return '<span class="bdg ' + c + '">' + t + '</span>'; }
function hash(s) { var h=0; for(var i=0;i<s.length;i++) h=(h*31+s.charCodeAt(i))&0xffff; return h.toString(16); }

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

/* ═══ ETL 同步触发 (v2 — 2026-04-30)
   第一性原理: 用戶點擊前需要兩個信息決策——(1) 後端是否正在跑（避免重複觸發）;
              (2) 上次成功完成是何時（決定是否需要再次觸發）。
   逆向思維: 不從「點擊就 POST」出發，而從「點擊前先預檢狀態」出發。
   奧卡姆剃刀: 時區轉換用瀏覽器原生 Intl.DateTimeFormat({timeZone:'Asia/Shanghai'})，
              零依賴；後端只返 UTC ISO，前端負責顯示。
   ─────────────────────────────────────────────────────────── */

// UTC ISO → 上海時區人類可讀格式（"2026-04-30 18:25:33 (上海)"）
function fmtShanghai(isoUtc) {
  if (!isoUtc) return null;
  try {
    var d = new Date(isoUtc);
    if (isNaN(d.getTime())) return null;
    var parts = new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year:  'numeric', month:  '2-digit', day:    '2-digit',
      hour:  '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).formatToParts(d);
    var lookup = {};
    parts.forEach(function(p) { lookup[p.type] = p.value; });
    return lookup.year + '-' + lookup.month + '-' + lookup.day + ' '
         + lookup.hour + ':' + lookup.minute + ':' + lookup.second + ' (上海)';
  } catch(e) { return null; }
}

async function fetchEtlStatus() {
  try {
    var res = await fetch(API + '/api/v1/etl/status', { credentials: 'same-origin' });
    if (!res.ok) return null;
    return await res.json();
  } catch(e) { return null; }
}

async function triggerETL() {
  var btn = $('btn-etl'), txt = $('etl-txt');
  if (btn.classList.contains('running')) return;
  // v4: 前端第二道防線（後端 require_system_admin 才是真實權限）
  if (!CU || !CU.is_system_admin) {
    alert('此操作僅限系統管理員');
    return;
  }

  // ─── 1. 預檢後端狀態 ───
  var status = await fetchEtlStatus();
  if (status && status.running) {
    var startedSh = fmtShanghai(status.started_at) || '未知時間';
    var by = status.triggered_by ? '\n觸發人：' + status.triggered_by : '';
    alert('⏳ ETL 正在後端執行中，請勿重複觸發。\n\n本次開始時間：' + startedSh + by
        + '\n\n（請等待當前任務結束後再操作）');
    return;
  }

  // ─── 2. 顯示上次完成時間 + 二次確認 ───
  var lastSh = status ? fmtShanghai(status.completed_at) : null;
  var lastLine;
  if (lastSh) {
    var statusTag = '';
    if (status && status.last_status === 'failed')  statusTag = '（上次失敗）';
    if (status && status.last_status === 'timeout') statusTag = '（上次超時）';
    var elapsed = (status && status.elapsed_seconds != null)
                ? '，耗時 ' + Number(status.elapsed_seconds).toFixed(1) + ' 秒' : '';
    lastLine = '上次成功完成：' + lastSh + elapsed + statusTag;
  } else if (status && status.last_status && status.last_status !== 'success') {
    var lastErr = status.last_error ? '\n失敗原因：' + status.last_error.slice(0, 200) : '';
    lastLine = '尚無成功完成記錄（上次狀態：' + status.last_status + '）' + lastErr;
  } else {
    lastLine = '尚無同步記錄（首次執行）';
  }

  if (!confirm('确认执行数据同步（ETL）？\n\n' + lastLine
             + '\n\n此操作将从 Excel 数据源重新清洗并写入数据库，'
             + '預計耗時 1-5 分鐘，期間請勿關閉頁面或重複點擊。')) {
    return;
  }

  // ─── 3. 執行 ───
  btn.classList.add('running');
  txt.textContent = '同步中...';
  try {
    var res = await fetch(API + '/api/v1/etl/trigger', {
      method: 'POST',
      credentials: 'same-origin'
    });
    var data = await res.json();
    if (res.ok) {
      txt.textContent = '✓ 完成';
      setTimeout(function(){ txt.textContent = '同步数据'; btn.classList.remove('running'); }, 2500);
      await fetchReport(origExecDate);
    } else if (res.status === 409) {
      // v4: 競態防御 — 預檢後到 POST 之間有人搶先觸發
      var d = data && data.detail;
      var msg = '⏳ ETL 已被其他請求觸發，請稍後重試';
      if (d && typeof d === 'object' && d.started_at) {
        msg += '\n\n本次開始時間：' + (fmtShanghai(d.started_at) || d.started_at);
        if (d.triggered_by) msg += '\n觸發人：' + d.triggered_by;
      }
      alert(msg);
      txt.textContent = '同步数据'; btn.classList.remove('running');
    } else {
      var detail = data && data.detail;
      var errMsg = (typeof detail === 'string') ? detail
                 : (detail && detail.message)   ? detail.message
                 : '未知错误';
      alert('ETL 执行失败: ' + errMsg);
      txt.textContent = '同步数据'; btn.classList.remove('running');
    }
  } catch(e) {
    alert('网络错误，请检查服务状态');
    txt.textContent = '同步数据'; btn.classList.remove('running');
  }
}

/* ═══ RENDER（不再有 scaleKpi / getMul — 后端已精确过滤）═══ */
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
