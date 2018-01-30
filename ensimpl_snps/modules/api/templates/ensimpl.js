;
///////////////////////////
// Create an immediately invoked functional expression to wrap our code
(function() {
    
        // Define our constructor
        this.ensimpl = function() {
    
            // Create global element references
            this.response = null;
    
            // Define option defaults
            this.defaults = {
                versions_url: "{{url_for('api.versions', _external=True)}}",
                search_exact: false,
                search_limit: 1000,
                search_species: null,
                search_url: "{{url_for('api.search', _external=True)}}",
                search_version: null
            };
    
            var $ = window.$ || window.JQuery;
    
            // Create options by extending defaults with the passed in arguments
            if (arguments[0] && typeof arguments[0] === "object") {
                this.settings = $.extend({}, this.defaults, arguments[0]);
            } else {
                this.settings = this.defaults;
            }
    
            // init anything else
            initialize.call(this);
        };
    
      // Public Methods
    
      ensimpl.prototype.reconfigure = function(settings) {
          console.log('reconfiguring');
          console.log('this.defaults=', this.defaults);
          var $ = window.$ || window.JQuery;
          this.settings = $.extend({}, this.defaults, settings);
      };
    
      ensimpl.prototype.versions = function(callback) {
          var _ = this;
          var $ = window.$ || window.JQuery;
          _.response = null;
    
          $.ajax({
              url: _.settings.versions_url,
              dataType: "jsonp",
              jsonp: 'callback',
              data: {'expand': 1}
          }).done(function (data, textStatus, jqXHR) {
              _.response = data;
    
              if (callback) {
                  callback(data);
              }
          }).fail(function (jqXHR, textStatus, errorThrown) {
              console.error('Ensimpl fail');
              console.error('jqXHR', jqXHR);
              console.error('textStatus', textStatus);
              console.error('errorThrown', errorThrown);
          });
      };
    
      ensimpl.prototype.search = function(term, options, callback) {
          var _ = this;
          var $ = window.$ || window.JQuery;
          _.response = null;
    
          var search_data = {
              term: term,
              species: _.settings.search_species,
              limit: _.settings.search_limit,
              exact: _.settings.search_exact,
              version: _.settings.version
          };
    
          if (options && typeof options === 'object') {
              search_data = $.extend({}, search_data, options);
          }
    
          $.ajax({
              url: _.settings.search_url,
              dataType: "jsonp",
              jsonp: 'callback',
              data: search_data
          }).done(function (data, textStatus, jqXHR) {
              _.response = data;
    
              if (callback) {
                  callback(data);
              }
          }).fail(function (jqXHR, textStatus, errorThrown) {
              console.error('Ensimpl fail');
              console.error('jqXHR', jqXHR);
              console.error('textStatus', textStatus);
              console.error('errorThrown', errorThrown);
          });
      };
    
      // Private Methods
    
      function initialize() {
            //console.log('check_jquery');
            var jquery = window.$ || window.JQuery;
            if (jquery === undefined || jquery.fn.jquery !== '2.0.3') {
                //loadfile(this.options.jquery_url, 'js');//, main);
                //console.log('jquery: loaded');
            } else {
                //main();
            }
    
      }
    
      function loadfile(filesrc, filetype, onload) {
        if (filetype == "js") { //if filename is a external JavaScript file
            var fileref = document.createElement('script');
            fileref.setAttribute("type", "text/javascript");
            fileref.setAttribute("src", filesrc);
            fileref.setAttribute("async", true)
        }
        else if (filetype == "css") { //if filename is an external CSS file
            var fileref = document.createElement("link");
            fileref.setAttribute("rel", "stylesheet");
            fileref.setAttribute("type", "text/css");
            fileref.setAttribute("href", filesrc);
            fileref.setAttribute("async", true)
        }
        if (typeof fileref != "undefined")
            if (fileref.readyState) {
                fileref.onreadystatechange = function () { // For old versions of IE
                    if (this.readyState == 'complete' || this.readyState == 'loaded') {
                        onload();
                    }
                };
            } else {
                fileref.onload = onload;
            }
    
        (document.getElementsByTagName('head')[0] || document.getElementsByTagName('body')[0]).appendChild(fileref)
    }
    
    
    //ref:http://www.tomhoppe.com/index.php/2008/03/dynamically-adding-css-through-javascript/
    function add_css(cssCode) {
        var styleElement = document.createElement("style");
        styleElement.type = "text/css";
        if (styleElement.styleSheet) {
            styleElement.styleSheet.cssText = cssCode;
        } else {
            styleElement.appendChild(document.createTextNode(cssCode));
        }
        document.getElementsByTagName("head")[0].appendChild(styleElement);
    }
    
    }());
    
    ///////////////////////////
    