document.querySelectorAll('.drop').forEach(e => {
  e.draggable = true;
  e.ondragstart = e => {
    e.dataTransfer.setData("id", e.target.id);
    e.target.classList.add('dragging');
  }
  e.ondragover = e => {
    let old = document.querySelector('.over');
    old && old.classList.remove('over')
    e.target.classList.add('over');
    e.preventDefault();
  };
  e.ondrop = e => {
    let old = document.querySelector('.dragging');
    old && old.classList.remove('dragging')
    old = document.querySelector('.over');
    old && old.classList.remove('over');
    let v = e.target.innerHTML;
    let fromEl = document.querySelector('#'+e.dataTransfer.getData('id'));
    e.target.innerHTML = fromEl.innerHTML;
    fromEl.innerHTML = v;

  };
})