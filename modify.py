import re

with open('/tmp/metaphysics-live/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# ============================================================
# 1. Replace CSS: Remove old city-grid/city-btn/district styles, add new select styles
# ============================================================

# Remove the old CSS block for .city-grid, .city-btn, .city-more (lines ~227-270 area)
old_css1 = r'/\* 城市选择 \*/\n\.city-grid\{[^}]+\}\n\.city-btn\{[^}]+\}\n\.city-btn:hover\{[^}]+\}\n\.city-btn\.selected\{[^}]+\}\n\.city-more\{[^}]+\}\n\.city-more:hover\{[^}]+\}\n\.city-more::before\{[^}]+\}\n\.city-more\.expanded::before\{[^}]*\}'
content = re.sub(old_css1, '/* 省市区三级联动选择 */\n.region-select-group{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap}\n.region-select-group .region-select{flex:1;min-width:0;padding:8px 10px;border:1px solid var(--border);border-radius:8px;background:rgba(45,32,69,.5);color:var(--text-light);font-size:13px;outline:none;cursor:pointer;transition:border-color .3s;appearance:auto}\n.region-select-group .region-select:focus{border-color:var(--accent);background:rgba(45,32,69,.8)}\n.region-select-group .region-select option{background:#1a1033;color:var(--text-light)}', content)

# Remove the minified CSS for city-grid and district-grid (line 602-609 area)
old_css2 = r'\.city-grid\{display:grid;grid-template-columns:repeat\(4,1fr\);gap:6px\}\.city-btn\{padding:8px 4px;border:1px solid var\(--border\);border-radius:8px;background:rgba\(45,32,69,\.5\);color:var\(--text-light\);font-size:12px;cursor:pointer;text-align:center;transition:all \.3s\}\.city-btn:hover\{border-color:var\(--accent\);background:rgba\(155,109,255,\.15\)\}\.city-btn\.selected\{border-color:var\(--accent\);background:rgba\(155,109,255,\.3\);color:var\(--accent-light\)\}\.city-more\{grid-column:1/-1;text-align:center;padding:6px;color:var\(--text-dim\);font-size:12px;cursor:pointer;border:1px dashed var\(--border\);border-radius:8px\}\.city-more:hover\{color:var\(--accent\);border-color:var\(--accent\)\}'
content = re.sub(old_css2, '.region-select-group{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap}.region-select-group .region-select{flex:1;min-width:0;padding:8px 10px;border:1px solid var(--border);border-radius:8px;background:rgba(45,32,69,.5);color:var(--text-light);font-size:13px;outline:none;cursor:pointer;transition:border-color .3s}.region-select-group .region-select:focus{border-color:var(--accent)}.region-select-group .region-select option{background:#1a1033;color:var(--text-light)}', content)

# Remove district-grid CSS (line 603)
old_css3 = r'\.district-grid\{display:grid;grid-template-columns:repeat\(5,1fr\);gap:4px;margin-top:6px;padding:8px;background:rgba\(155,109,255,\.05\);border-radius:8px;border:1px solid rgba\(155,109,255,\.2\)\}'
content = re.sub(old_css3, '', content)

# Remove district-btn CSS
old_css4 = r'\n\.district-btn\{[^}]+\}\n\.district-btn:hover\{[^}]+\}\n\.district-btn\.selected\{[^}]+\}\n\.district-back\{[^}]+\}\n\.district-back:hover\{[^}]+\}'
content = re.sub(old_css4, '\n', content)

# Remove @media for district-grid
content = re.sub(r'@media\(max-width:768px\)\{\.district-grid\{grid-template-columns:repeat\(3,1fr\)\}\}', '@media(max-width:768px){.region-select-group{flex-direction:column}}', content)


# ============================================================
# 2. Replace HTML: 4 city-grid + 4 district-grid → 4 sets of 3 selects
# ============================================================

# Helper to generate the replacement HTML
def make_region_selects(prefix):
    return f'''<div class="region-select-group" id="{prefix}RegionGroup">
      <select class="region-select" id="{prefix}Province" data-region-prefix="{prefix}" data-level="province" onchange="onProvinceChange('{prefix}')"><option value="">省份</option></select>
      <select class="region-select" id="{prefix}City" data-region-prefix="{prefix}" data-level="city" onchange="onCityChange('{prefix}')" disabled><option value="">城市</option></select>
      <select class="region-select" id="{prefix}District" data-region-prefix="{prefix}" data-level="district" disabled><option value="">区县</option></select>
    </div>'''

# Replace male birth city
content = content.replace(
    '<div class="city-grid" id="deepMaleBirthCity" style="margin-bottom:8px"></div>\n    <div class="district-grid" id="deepMaleBirthCityDistrict" style="display:none"></div>',
    make_region_selects('deepMaleBirth')
)

# Replace male current city
content = content.replace(
    '<div class="city-grid" id="deepMaleCurrentCity"></div>\n    <div class="district-grid" id="deepMaleCurrentCityDistrict" style="display:none"></div>',
    make_region_selects('deepMaleCurrent')
)

# Replace female birth city
content = content.replace(
    '<div class="city-grid" id="deepFemaleBirthCity" style="margin-bottom:8px"></div>\n    <div class="district-grid" id="deepFemaleBirthCityDistrict" style="display:none"></div>',
    make_region_selects('deepFemaleBirth')
)

# Replace female current city
content = content.replace(
    '<div class="city-grid" id="deepFemaleCurrentCity"></div>\n    <div class="district-grid" id="deepFemaleCurrentCityDistrict" style="display:none"></div>',
    make_region_selects('deepFemaleCurrent')
)

# ============================================================
# 3. Replace JS: Remove old constants & functions, add new cascade logic
# ============================================================

# Remove CITIES_BASIC and CITIES_MORE
content = re.sub(
    r"const CITIES_BASIC = \[.*?\];\nconst CITIES_MORE = \[.*?\];",
    "// 省市区数据通过阿里DataV API动态加载",
    content
)

# Remove CITY_DISTRICTS (large block from 'const CITY_DISTRICTS = {' to the closing '};')
content = re.sub(
    r"const CITY_DISTRICTS = \{[\s\S]*?\n\};",
    "// CITY_DISTRICTS 已移除，改用API加载省市区数据",
    content
)

# Remove initCities function
content = re.sub(
    r"function initCities\(\)\{[\s\S]*?\n\}\n\nfunction toggleMoreCities[\s\S]*?\n\}",
    """// 省市区三级联动 - 使用阿里DataV API
const REGION_CACHE = {};

async function loadRegionData(adcode) {
  if (REGION_CACHE[adcode]) return REGION_CACHE[adcode];
  try {
    const resp = await fetch(`https://geo.datav.aliyun.com/areas_v3/bound/${adcode}_full.json`);
    const data = await resp.json();
    const items = data.features.map(f => ({
      name: f.properties.name,
      adcode: f.properties.adcode
    })).sort((a, b) => a.adcode - b.adcode);
    REGION_CACHE[adcode] = items;
    return items;
  } catch (e) {
    console.error('加载区域数据失败:', adcode, e);
    return [];
  }
}

async function initRegionSelectors() {
  const prefixes = ['deepMaleBirth','deepMaleCurrent','deepFemaleBirth','deepFemaleCurrent'];
  // Load provinces once
  const provinces = await loadRegionData('100000');
  prefixes.forEach(prefix => {
    const sel = document.getElementById(prefix + 'Province');
    if (!sel) return;
    sel.innerHTML = '<option value="">选择省份</option>' +
      provinces.map(p => `<option value="${p.adcode}">${p.name}</option>`).join('');
  });
}

async function onProvinceChange(prefix) {
  const provSel = document.getElementById(prefix + 'Province');
  const citySel = document.getElementById(prefix + 'City');
  const distSel = document.getElementById(prefix + 'District');
  const adcode = provSel.value;
  // Reset city & district
  citySel.innerHTML = '<option value="">选择城市</option>';
  citySel.disabled = true;
  distSel.innerHTML = '<option value="">区县</option>';
  distSel.disabled = true;
  if (!adcode) return;
  const cities = await loadRegionData(adcode);
  if (cities.length === 0) {
    // 直辖市等没有下级城市，直接用区县填充城市列
    citySel.innerHTML = '<option value="">选择区县</option>';
    distSel.innerHTML = '<option value="">--</option>';
    distSel.disabled = true;
    // 尝试加载该省的区县（对于直辖市如北京110000）
    const districts = await loadRegionData(adcode);
    // 对于直辖市，API返回的就是区县
    if (districts.length > 0 && cities.length === 0) {
      citySel.innerHTML = '<option value="">选择区县</option>' +
        districts.map(d => `<option value="${d.adcode}">${d.name}</option>`).join('');
      citySel.disabled = false;
    }
    return;
  }
  citySel.innerHTML = '<option value="">选择城市</option>' +
    cities.map(c => `<option value="${c.adcode}">${c.name}</option>`).join('');
  citySel.disabled = false;
}

async function onCityChange(prefix) {
  const citySel = document.getElementById(prefix + 'City');
  const distSel = document.getElementById(prefix + 'District');
  const adcode = citySel.value;
  distSel.innerHTML = '<option value="">区县</option>';
  distSel.disabled = true;
  if (!adcode) return;
  const districts = await loadRegionData(adcode);
  if (districts.length > 0) {
    distSel.innerHTML = '<option value="">选择区县</option>' +
      districts.map(d => `<option value="${d.adcode}">${d.name}</option>`).join('');
    distSel.disabled = false;
  }
}""",
    content
)

# Remove selectCity, selectDistrict, resetDistrict functions
content = re.sub(
    r"\nfunction selectCity\(gridId,cityName\)\{[\s\S]*?\n\}\n\}\n",
    "\n",
    content
)
content = re.sub(
    r"\nfunction selectDistrict\(gridId,districtName\)\{[\s\S]*?\n\}\n",
    "\n",
    content
)
content = re.sub(
    r"\nfunction resetDistrict\(gridId\)\{[\s\S]*?\n\}\n",
    "\n",
    content
)

# Replace getSelectedCity function
old_getSelectedCity = r"""function getSelectedCity(gridId){
const grid=document.getElementById(gridId);
let city='';
if(grid)city=grid.querySelector('.city-btn.selected')?.dataset.city||'';
const districtGrid=document.getElementById(gridId+'District');
let district='';
if(districtGrid){
const sel=districtGrid.querySelector('.district-btn.selected');
if(sel)district=sel.dataset.district||'';
}
if(city&&district)return city+' '+district;
return city;
}"""

new_getSelectedCity = """function getSelectedCity(prefix) {
  const provSel = document.getElementById(prefix + 'Province');
  const citySel = document.getElementById(prefix + 'City');
  const distSel = document.getElementById(prefix + 'District');
  if (!provSel) return '';
  const provText = provSel.options[provSel.selectedIndex]?.text || '';
  const cityText = citySel?.options[citySel.selectedIndex]?.text || '';
  const distText = distSel?.options[distSel.selectedIndex]?.text || '';
  // Build location string
  let parts = [];
  if (provText && provText !== '选择省份') parts.push(provText);
  if (cityText && cityText !== '选择城市' && cityText !== '选择区县') parts.push(cityText);
  if (distText && distText !== '区县' && distText !== '选择区县') parts.push(distText);
  return parts.join(' ');
}"""

content = content.replace(old_getSelectedCity, new_getSelectedCity)

# Update all calls to getSelectedCity that pass grid IDs like 'deepMaleBirthCity'
# They should now pass prefixes like 'deepMaleBirth'
content = content.replace("getSelectedCity('deepMaleBirthCity')", "getSelectedCity('deepMaleBirth')")
content = content.replace("getSelectedCity('deepFemaleBirthCity')", "getSelectedCity('deepFemaleBirth')")
content = content.replace("getSelectedCity('deepMaleCurrentCity')", "getSelectedCity('deepMaleCurrent')")
content = content.replace("getSelectedCity('deepFemaleCurrentCity')", "getSelectedCity('deepFemaleCurrent')")

# Replace initCities() call with initRegionSelectors()
content = content.replace('initCities()', 'initRegionSelectors()')

with open('/tmp/metaphysics-live/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Done! File modified successfully.")
