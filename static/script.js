"use strict";
function submitForm(event) {
    event.preventDefault();
    var form = document.getElementById("ozonForm");
    var formData = new FormData(form);
    var xhr = new XMLHttpRequest();
    xhr.open("POST", "/ozon", true);
    xhr.send(formData);

    clearForm();
}

function clearForm() {
    document.getElementById("ozonForm").reset();
    document.getElementById("successMessage").style.display = "block";
    setTimeout(function() {
        document.getElementById("successMessage").style.display = "none";
    }, 2000);  // Задержка в 2000 миллисекунд (2 секунды) перед скрытием сообщения
}
