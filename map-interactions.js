(() => {
  const frame = document.querySelector("#map-frame");
  const stage = document.querySelector(".map-stage");
  if (!frame || !stage) return;

  const MIN_ZOOM = 0.78;
  const MAX_ZOOM = 15;
  const PAN_BASE_X = 360;
  const PAN_BASE_Y = 260;
  const PAN_EXTRA_X = 820;
  const PAN_EXTRA_Y = 620;

  const state = {
    x: 0,
    y: 0,
    zoom: 1,
    drag: null,
    suppressClickUntil: 0,
  };

  const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

  function selectableKey(target) {
    return target?.closest?.(".lga-boundary,.count-marker")?.dataset?.key || "";
  }

  function maxPanForZoom(zoom) {
    const extra = Math.max(0, zoom - 1);
    return {
      x: PAN_BASE_X + extra * PAN_EXTRA_X,
      y: PAN_BASE_Y + extra * PAN_EXTRA_Y,
    };
  }

  function applyTransform() {
    const bounds = maxPanForZoom(state.zoom);
    state.x = clamp(state.x, -bounds.x, bounds.x);
    state.y = clamp(state.y, -bounds.y, bounds.y);
    frame.style.setProperty("--map-pan-x", `${state.x.toFixed(1)}px`);
    frame.style.setProperty("--map-pan-y", `${state.y.toFixed(1)}px`);
    const counterScale = 1 / state.zoom;
    frame.style.setProperty("--map-user-scale", state.zoom.toFixed(3));
    frame.style.setProperty("--map-counter-scale", counterScale.toFixed(4));

  }

  function setTransform({ x = state.x, y = state.y, zoom = state.zoom } = {}) {
    state.zoom = clamp(zoom, MIN_ZOOM, MAX_ZOOM);
    state.x = x;
    state.y = y;
    applyTransform();
  }

  function zoomAt(clientX, clientY, nextZoom) {
    const previousZoom = state.zoom;
    const zoom = clamp(nextZoom, MIN_ZOOM, MAX_ZOOM);
    if (Math.abs(zoom - previousZoom) < 0.001) return;

    const rect = frame.getBoundingClientRect();
    const ratio = zoom / previousZoom;
    const anchorX = clientX - (rect.left + rect.width / 2);
    const anchorY = clientY - (rect.top + rect.height / 2);

    setTransform({
      zoom,
      x: state.x - anchorX * (ratio - 1),
      y: state.y - anchorY * (ratio - 1),
    });
  }

  function onWheel(event) {
    event.preventDefault();
    zoomAt(event.clientX, event.clientY, state.zoom * Math.exp(-event.deltaY * 0.0012));
  }

  function beginPan(event) {
    if (event.button !== 0 || event.target.closest(".map-tools")) return;
    state.drag = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      panX: state.x,
      panY: state.y,
      moved: false,
      targetKey: selectableKey(event.target),
    };
    frame.classList.add("is-panning");
    frame.setPointerCapture?.(event.pointerId);
  }

  function movePan(event) {
    if (!state.drag || state.drag.pointerId !== event.pointerId) return;
    event.preventDefault();
    const dx = event.clientX - state.drag.startX;
    const dy = event.clientY - state.drag.startY;
    if (Math.hypot(dx, dy) > 4) state.drag.moved = true;
    setTransform({ x: state.drag.panX + dx, y: state.drag.panY + dy });
  }

  function endPan(event) {
    if (!state.drag || state.drag.pointerId !== event.pointerId) return;
    const moved = state.drag.moved;
    const targetKey = state.drag.targetKey;
    if (state.drag.moved) {
      state.suppressClickUntil = performance.now() + 220;
    }
    state.drag = null;
    frame.classList.remove("is-panning");
    frame.releasePointerCapture?.(event.pointerId);
    if (!moved && targetKey && typeof window.JOBSIGHT_SELECT_COUNCIL === "function") {
      event.preventDefault();
      state.suppressClickUntil = performance.now() + 180;
      window.JOBSIGHT_SELECT_COUNCIL(targetKey);
    }
  }

  function suppressDragClick(event) {
    if (performance.now() >= state.suppressClickUntil) return;
    event.preventDefault();
    event.stopImmediatePropagation();
  }

  function addControls() {
    const controls = document.createElement("div");
    controls.className = "map-tools";
    controls.setAttribute("aria-label", "Map zoom controls");
    controls.innerHTML = [
      '<button type="button" data-map-action="zoom-in" aria-label="Zoom in">+</button>',
      '<button type="button" data-map-action="zoom-out" aria-label="Zoom out">-</button>',
      '<button type="button" data-map-action="reset" aria-label="Reset map zoom">1:1</button>',
    ].join("");
    controls.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-map-action]");
      if (!button) return;
      event.preventDefault();
      event.stopPropagation();
      const rect = frame.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const centerY = rect.top + rect.height / 2;
      if (button.dataset.mapAction === "zoom-in") zoomAt(centerX, centerY, state.zoom * 1.22);
      if (button.dataset.mapAction === "zoom-out") zoomAt(centerX, centerY, state.zoom / 1.22);
      if (button.dataset.mapAction === "reset") setTransform({ x: 0, y: 0, zoom: 1 });
    });
    stage.appendChild(controls);
  }

  const style = document.createElement("style");
  style.textContent = `
    .map-frame{
      --map-pan-x:0px;
      --map-pan-y:0px;
      --map-user-scale:1;
      transform:translate(var(--map-pan-x),var(--map-pan-y)) scale(var(--map-user-scale));
      transform-origin:center center;
      transition:transform 150ms ease;
      touch-action:none;
      user-select:none;
    }
    .map-frame.is-panning{cursor:grabbing;transition:none}
    .map-tools{
      position:absolute;
      z-index:8;
      right:18px;
      top:18px;
      display:flex;
      gap:6px;
      padding:5px;
      border:1px solid var(--line);
      background:rgba(8,14,13,.76);
      backdrop-filter:blur(14px);
      box-shadow:0 14px 34px rgba(0,0,0,.26);
    }
    .map-tools button{
      min-width:34px;
      height:32px;
      border:1px solid rgba(216,231,222,.2);
      background:rgba(255,255,255,.07);
      color:var(--text);
      font-weight:900;
      cursor:pointer;
    }
    .map-tools button:hover,
    .map-tools button:focus-visible{
      border-color:var(--cool);
      background:rgba(52,213,194,.16);
      outline:none;
    }
    @media(max-width:640px){
      .map-tools{right:12px;top:12px}
      .map-tools button{min-width:31px;height:30px}
    }
  `;
  document.head.appendChild(style);

  stage.addEventListener("click", suppressDragClick, true);
  frame.addEventListener("pointerdown", beginPan);
  frame.addEventListener("pointermove", movePan);
  frame.addEventListener("pointerup", endPan);
  frame.addEventListener("pointercancel", endPan);
  frame.addEventListener("wheel", onWheel, { passive: false });
  addControls();
  applyTransform();
})();
