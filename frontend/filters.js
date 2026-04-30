/* ============================================================
 * filters.js — 篩選器系統
 * ------------------------------------------------------------
 * 職責：
 *   1. 自定義下拉組件（dd*）— 可搜索、多選、外部點擊收起
 *   2. 篩選器構建（buildOptions）+ 級聯（rebuild* / cascade*）
 *   3. 篩選器應用（applyFilters / clearFilters / rmFilter / renderTags）
 *   4. 時間篩選器解析（computeExecDate）
 *   5. 手機端手風琴折疊（setupMobileAccordion / updateAccordionBadges）
 *
 * 依賴 core.js：$、CU、RAW、ALL_ADVS、FO、origExecDate、fmtD
 * 被 app.js 調用：buildOptions、setupMobileAccordion、applyFilters
 * 被 HTML onclick 調用：ddToggle、applyFilters、clearFilters
 * ============================================================ */

/* ═══ 篩選器狀態 ═══ */
var fLine=[], fSubLine=[], fGSys=[], fBlock=[], fGL1=[], fGAdv=[], fBType=[], fA=[], fFY=[], fYM=[], fFW=[];

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
