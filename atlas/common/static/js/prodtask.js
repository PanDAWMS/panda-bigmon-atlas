        function construct_django_url(name,args) {
            if (args === undefined) {
                return name;
            }
            else{
                var argsArray = Array.prototype.slice.apply(arguments, [1, arguments.length]);
                return name+argsArray.join('/')+'/';
            }

        }